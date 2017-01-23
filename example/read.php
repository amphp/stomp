<?php

require __DIR__ . "/../vendor/autoload.php";

error_reporting(E_ALL);

$uri = "tcp://guest:guest@localhost:61613";
$client = new Amp\Stomp\Client($uri);

Amp\run(function () use ($client) {
    yield $client->connect();

    // schedule a message send every half second
    Amp\repeat(function () use ($client) {
        yield $client->send("/exchange/stomp-test/foo.bar", "mydata");
    }, 500);

    // subscribe to the messages we're sending
    $subscriptionId = $client->subscribe("/exchange/stomp-test/*.*");

    // dump all messages we receive to the console
    while (true) {
        echo yield $client->read(), "\n\n";
    }
});
