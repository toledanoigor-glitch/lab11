#!/usr/bin/env php
<?php

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use Algolia\AlgoliaSearch\Api\SearchClient;

$defaults = [
    'host' => '127.0.0.1',
    'port' => '3306',
    'db' => 'movies_db',
    'user' => 'root',
    'pass' => '',
    'table' => 'moviedb',
    'index' => 'movies',
    'appId' => '7EYHMLXK5T',
    'apiKey' => '65de2811c569d5219d857a8221367169',
    'batch' => '1000',
];

$options = getopt('', [
    'host:',
    'port:',
    'db:',
    'user:',
    'pass:',
    'table:',
    'index:',
    'appId:',
    'apiKey:',
    'batch:',
    'dry-run',
]);

$config = $defaults;
foreach (['host','port','db','user','pass','table','index','appId','apiKey','batch'] as $key) {
    if (isset($options[$key])) {
        $config[$key] = $options[$key];
    }
}

$dryRun = isset($options['dry-run']);

function fail(string $message): void
{
    fwrite(STDERR, "ERROR: {$message}\n");
    exit(1);
}

if ($config['appId'] === '' || $config['apiKey'] === '') {
    fail('Algolia Application ID and Admin API Key must be provided.');
}

$tableName = preg_replace('/[^a-zA-Z0-9_]/', '', $config['table']);
$indexName = preg_replace('/[^a-zA-Z0-9_\-]/', '', $config['index']);
$batchSize = max(1, (int) $config['batch']);

$dsn = sprintf('mysql:host=%s;port=%s;dbname=%s;charset=utf8mb4', $config['host'], $config['port'], $config['db']);

try {
    $pdo = new PDO($dsn, $config['user'], $config['pass'], [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::ATTR_EMULATE_PREPARES => false,
    ]);
} catch (PDOException $ex) {
    fail('Database connection failed: ' . $ex->getMessage());
}

$sql = sprintf(
    'SELECT id, release_date, title, overview, vote_average, original_language, genre, poster_url FROM `%s`',
    $tableName
);

try {
    $stmt = $pdo->query($sql);
    $rows = $stmt->fetchAll();
} catch (PDOException $ex) {
    fail('Query failed: ' . $ex->getMessage());
}

$totalRows = count($rows);
if ($totalRows === 0) {
    echo "No records found in table {$tableName}.\n";
    exit(0);
}

$objects = array_map(static function (array $row): array {
    $releaseYear = null;
    if (!empty($row['release_date']) && preg_match('/^(\d{4})/', $row['release_date'], $matches)) {
        $releaseYear = (int) $matches[1];
    }

    $genreList = [];
    if (!empty($row['genre'])) {
        $genreList = array_filter(array_map('trim', explode(',', $row['genre'])));
    }

    return [
        'objectID' => $row['id'],
        'release_date' => $row['release_date'],
        'release_year' => $releaseYear,
        'title' => $row['title'],
        'overview' => $row['overview'],
        'vote_average' => (float) $row['vote_average'],
        'original_language' => $row['original_language'],
        'genre' => $row['genre'],
        'genres' => array_values($genreList),
        'poster_url' => $row['poster_url'],
    ];
}, $rows);

echo "Found {$totalRows} movie records in {$tableName}.\n";
echo "Preparing to sync to Algolia index '{$indexName}' using App ID {$config['appId']}.\n";

if ($dryRun) {
    echo "Dry run enabled. The script will not send any records to Algolia.\n";
    exit(0);
}

try {
    $client = SearchClient::create($config['appId'], $config['apiKey']);
} catch (Throwable $ex) {
    fail('Algolia client initialization failed: ' . $ex->getMessage());
}

try {
    $client->setSettings($indexName, [
        'attributesForFaceting' => [
            'genres',
            'release_year',
        ],
    ]);
    echo "Index settings updated to support genre and year filtering.\n";
} catch (Throwable $ex) {
    fail('Algolia settings update failed: ' . $ex->getMessage());
}

$batchCount = 0;
$sent = 0;
$startTime = microtime(true);
foreach (array_chunk($objects, $batchSize) as $chunk) {
    $batchCount++;
    $requests = array_map(static function (array $object): array {
        return [
            'action' => 'addObject',
            'body' => $object,
        ];
    }, $chunk);

    try {
        $response = $client->batch($indexName, ['requests' => $requests]);
    } catch (Throwable $ex) {
        fail('Algolia batch upload failed on batch ' . $batchCount . ': ' . $ex->getMessage());
    }

    $sent += count($chunk);
    echo sprintf("Batch %d uploaded %d records.\n", $batchCount, count($chunk));
}

$duration = round(microtime(true) - $startTime, 2);
echo sprintf("Sync complete: %d records sent in %d batches over %s seconds.\n", $sent, $batchCount, $duration);

exit(0);
