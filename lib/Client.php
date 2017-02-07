<?php

namespace Amp\Stomp;

use AsyncInterop\Loop;
use AsyncInterop\Promise;

use Amp\Success;
use Amp\Failure;
use Amp\Deferred;
use Amp\Coroutine;
use Amp\Emitter;
use Amp\Stream;

class Client
{
    const OPTIONS = [
        "default_content_type"  => 0,
        "require_receipt"       => 1,
        "force_connect_frame"   => 2,
        "vhost"                 => 3,
        "login"                 => 4,
        "passcode"              => 5,
        "accept_version"        => 6,
        "heart_beat_enable"     => 7,
        "heart_beat_min"        => 8,
        "heart_beat_max"        => 9,
        "universal_headers"     => 10,
    ];

    const ALLOWED_ACKS = [
        "auto" => "auto",
        "client" => "client",
        "client-individual" => "client-individual",
    ];

    const DEFAULT_PORT = "61613";

    private $uri;
    private $socket;
    private $frameStream;
    private $receiptsInWaiting = [];
    private $connectionPromise;
    private $filter;
    private $heartbeatWatcher;
    private $lastDataSentAt;
    private $versionInUse;
    private $subscriptionIdToEmitterMap = [];
    private $streamIdToSubscriptionIdMap;

    private $options = [
        self::OPTIONS["default_content_type"]   => "text/plain",
        self::OPTIONS["require_receipt"]        => false,
        self::OPTIONS["force_connect_frame"]    => false,
        self::OPTIONS["vhost"]                  => "/",
        self::OPTIONS["login"]                  => null,
        self::OPTIONS["passcode"]               => null,
        self::OPTIONS["accept_version"]         => Version::V_ALL,
        self::OPTIONS["heart_beat_enable"]      => true,
        self::OPTIONS["heart_beat_min"]         => 1000,
        self::OPTIONS["heart_beat_max"]         => 3000,
        self::OPTIONS["universal_headers"]      => [],
    ];

    /**
     * @param string $uri {scheme}://{host}:{port}{vhost}
     */
    public function __construct(string $uri, array $options = [], Filter $filter = null)
    {
        $uriParts = \parse_url($uri);
        $port = $uriParts["port"] ?? self::DEFAULT_PORT;
        $this->uri = "{$uriParts["scheme"]}://{$uriParts["host"]}:{$port}";

        if (isset($uriParts["path"]) && !isset($options[self::OPTIONS["vhost"]])) {
            $options[self::OPTIONS["vhost"]] = $uriParts["path"];
        }
        if (isset($uriParts["user"]) && !isset($options[self::OPTIONS["login"]])) {
            $options[self::OPTIONS["login"]] = $uriParts["user"];
        }
        if (isset($uriParts["pass"]) && !isset($options[self::OPTIONS["passcode"]])) {
            $options[self::OPTIONS["passcode"]] = $uriParts["pass"];
        }

        $this->setOptions($options);
        $this->filter = $filter ?? new NullFilter;
        $this->streamIdToSubscriptionIdMap = new \SplObjectStorage;
    }

    private function setOptions(array $options)
    {
        // @TODO actually validate and assign these
        foreach ($options as $key => $value) {
            $this->options[$key] = $value;
        }
    }

    /**
     * @return Promise<null>
     */
    public function connect(): Promise
    {
        if ($this->socket) {
            return new Success;
        }
        if ($this->connectionPromise) {
            return $this->connectionPromise;
        }

        $deferred = new Deferred;
        $connectionPromise = new Coroutine($this->doConnect());
        $connectionPromise->when(function ($e) use ($deferred) {
            $this->connectionPromise = null;
            if ($e) {
                $this->end();
                $deferred->fail($e);
            } else {
                $deferred->resolve();
            }
        });

        $this->connectionPromise = $connectionPromise;

        return $deferred->promise();
    }

    private function doConnect(): \Generator
    {
        $this->socket = $socket = yield \Amp\Socket\connect($this->uri);
        $this->frameStream = new FrameStream($socket);

        $command = $this->buildConnectCommand();
        $headers = $this->buildConnectHeaders();
        $frame = new Frame($command, $headers, $body = "");

        yield from $this->sendFrame($frame);

        yield $this->frameStream->advance();
        $frame = $this->frameStream->getCurrent();

        list($command, $headers, $body) = $frame->list();

        if ($command !== Command::CONNECTED) {
            throw new StompException(
                "Expected CONNECTED frame, received {$command} frame instead"
            );
        }

        $this->versionInUse = isset($headers["version"])
            ? $this->determineNegotiatedVersion($headers["version"])
            : Version::STRINGS[Version::V_1_0]
        ;

        if (isset($headers["heart-beat"])) {
            $this->initializeHeartbeat($headers["heart-beat"]);
        }

        // consume data off the frame stream now that we're all setup
        new Coroutine($this->consume());
    }

    private function sendFrame(Frame $frame): \Generator
    {
        $frame = $this->filter->filter(Filter::WRITE, $frame);
        list($command, $headers, $body) = $frame->list();

        foreach ($this->options[self::OPTIONS["universal_headers"]] as $key => $value) {
            if (!isset($headers[$key])) {
                $headers[$key] = $value;
            }
        }

        if ($command !== Command::CONNECT
            && !isset($headers["receipt"])
            && $this->options[Client::OPTIONS["require_receipt"]]
        ) {
            $headers["receipt"] = generateId();
        }

        $normalizedHeaders = $this->normalizeHeaders($headers);
        $dataToWrite = "{$command}\n{$normalizedHeaders}\n{$body}\0";

        yield $this->frameStream->send($dataToWrite);

        if (!isset($normalizedHeaders["receipt"])) {
            return;
        }

        $deferred = new Deferred;
        $this->receiptsInWaiting[$normalizedHeaders["receipt"]] = $deferred;

        yield $deferred->promise();
    }

    private function buildConnectCommand(): string
    {
        if ($this->options[self::OPTIONS["force_connect_frame"]]) {
            return Command::CONNECT;
        }
        if ($this->options[self::OPTIONS["accept_version"]] === Version::V_1_2) {
            return Command::STOMP;
        }
        return Command::CONNECT;
    }

    private function buildConnectHeaders(): array
    {
        $headers = [];
        if (!isset($headers["login"]) && $this->options[self::OPTIONS["login"]]) {
            $headers["login"] = $this->options[self::OPTIONS["login"]];
        }
        if (!isset($headers["passcode"]) && $this->options[self::OPTIONS["passcode"]]) {
            $headers["passcode"] = $this->options[self::OPTIONS["passcode"]];
        }

        $heartbeatMin = $this->options[self::OPTIONS["heart_beat_enable"]]
            ? $this->options[self::OPTIONS["heart_beat_min"]]
            : 0
        ;
        $heartbeatMax = $this->options[self::OPTIONS["heart_beat_max"]];
        $headers["heart-beat"] = "{$heartbeatMin},{$heartbeatMax}";

        $headers["host"] = $this->options[self::OPTIONS["vhost"]];

        $acceptedVersions = [];
        if ($this->options[self::OPTIONS["accept_version"]] & Version::V_1_0) {
            $acceptedVersions[] = Version::STRINGS[Version::V_1_0];
        }
        if ($this->options[self::OPTIONS["accept_version"]] & Version::V_1_1) {
            $acceptedVersions[] = Version::STRINGS[Version::V_1_1];
        }
        if ($this->options[self::OPTIONS["accept_version"]] & Version::V_1_2) {
            $acceptedVersions[] = Version::STRINGS[Version::V_1_2];
        }

        $headers["accept-version"] = \implode(",", $acceptedVersions);

        return $headers;
    }

    private function determineNegotiatedVersion(string $versionHeader): int
    {
        if ($version = \array_search($versionHeader, Version::STRINGS)) {
            return $version;
        }

        throw new StompException(
            "STOMP protocol version could not be determined"
        );
    }

    private function initializeHeartbeat(string $rcvdHeader)
    {
        if (\strpos($rcvdHeader, ",") === false) {
            return;
        }
        list($remoteMin, $remoteMax) = \array_map("trim", \explode(",", $rcvdHeader));
        if (empty($remoteMin)) {
            return;
        }

        $localMax = $this->options[self::OPTIONS["heart_beat_max"]];
        $max = ($remoteMax > $localMax) ? $remoteMax : $localMax;
        $this->heartbeatWatcher = Loop::repeat(1000, function () use ($max) {
            $this->doHeartbeat($max); 
        });
    }

    private function doHeartbeat(int $interval)
    {
        // if the heartbeat timout has yet to elapse we don't need to send
        $now = \microtime(true);
        $msSinceLastSend = (($now - $this->lastDataSentAt) * 1000);
        if ($msSinceLastSend < ($interval - 1000)) {
            return;
        }

        $this->frameStream->send("\n");
    }

    private function end(\Throwable $e = null)
    {
        if (isset($this->heartbeatWatcher)) {
            Loop::cancel($this->heartbeatWatcher);
        }

        if ($e) {
            foreach ($this->subscriptionIdToEmitterMap as $emitter) {
                $emitter->fail($e);
            }
        } else {
            foreach ($this->subscriptionIdToEmitterMap as $emitter) {
                $emitter->resolve();
            }
        }

        $this->socket = null;
        $this->frameStream = null;
        $this->heartbeatWatcher = null;
    }

    private function normalizeHeaders(array $headers)
    {
        $normalizedHeaders = "";
        foreach ($headers as $key => $value) {
            $value = $this->escapeHeaderValue($value);
            $normalizedHeaders .= "{$key}:{$value}\n";
        }

        return $normalizedHeaders;
    }

    private function escapeHeaderValue($value)
    {
        return \strtr($value, [
            "\n" => '\n',
            ':'  => '\c',
            '\\' => '\\\\',
        ]);
    }

    private function consume()
    {
        while (yield $this->frameStream->advance()) {
            $frame = $this->frameStream->getCurrent();
            $frame = $this->filter->filter(Filter::READ, $frame);

            switch ($frame->getCommand()) {
                case Command::MESSAGE:
                    $this->processMessage($frame);
                    break;
                case Command::RECEIPT:
                    $this->processReceipt($frame);
                    break;
                case Command::ERROR:
                    $this->end(new StompException(
                        "ERROR frame received:\n\n{$frame}"
                    ));
                    break;
                default:
                    $this->end(new StompException(
                        "Unknown frame type received:\n\n{$frame}"
                    ));
                    break;
            }
        }
    }

    private function processReceipt(Frame $frame)
    {
        $headers = $frame->getHeaders();
        if (!isset($headers["receipt-id"])) {
            return;
        }
        $receiptId = $headers["receipt-id"];
        if (!isset($this->receiptsInWaiting[$receiptId])) {
            return;
        }
        $toSucceed = [];
        foreach ($this->receiptsInWaiting as $key => $deferred) {
            $toSucceed[] = $deferred;
            unset($this->receiptsInWaiting[$key]);
            if ($key === $receiptId) {
                break;
            }
        }
        foreach ($toSucceed as $deferred) {
            $deferred->resolve();
        }
    }

    private function processMessage(Frame $frame)
    {
        $headers = $frame->getHeaders();
        if (!isset($headers["subscription"])) {
            // if there's no subscription ID in the headers we're done here
            return;
        }
        $subscriptionId = $headers["subscription"];
        $emitter = $this->subscriptionIdToEmitterMap[$subscriptionId];
        $emitter->emit($frame);
    }

    /**
     *
     */
    public function publish(string $destination, string $data, array $headers = []): Promise
    {
        $command = Command::SEND;
        $headers = \array_change_key_case($headers, \CASE_LOWER);
        $headers["destination"] = $headers["destination"] ?? $destination;
        $headers["content-type"] = $headers["content-type"] ?? $this->options[self::OPTIONS["default_content_type"]];
        if (!isset($headers["content-length"]) && isset($data[0])) {
            $headers["content-length"] = \strlen($data);
        }

        $body = $data;
        $frame = new Frame($command, $headers, $body);

        return new Coroutine($this->sendFrame($frame));
    }

    /**
     * @return Promise<Stream>
     */
    public function subscribe(string $destination, array $headers = []): Stream
    {
        $emitter = new Emitter;
        new Coroutine($this->establishSubscription($emitter, $destination, $headers));
        $stream = $emitter->stream();
        $streamId = \spl_object_hash($stream);
        $stream->when(function () use ($streamId) {
            unset($this->streamIdToSubscriptionIdMap[$streamId]);
        });

        return $stream;
    }

    private function establishSubscription(Emitter $emitter, string $destination, array $headers)
    {
        $ack = $headers["ack"] ?? "auto";
        if (!isset(self::ALLOWED_ACKS[$ack])) {
            $emitter->fail(new StompException(
                "Invalid ack header value: {$ack}"
            ));
            return;
        }

        if ($this->versionInUse === Version::V_1_0 &&
            $ack === self::ALLOWED_ACKS["client-individual"]
        ) {
            $emitter->fail(new StompException(
                "STOMP v1.0 does not support the use of ack:client-individual ... " .
                "Please use ack:auto or ack:client instead."
            ));
            return;
        }

        $command = Command::SUBSCRIBE;
        $headers = \array_change_key_case($headers, \CASE_LOWER);
        $headers["destination"] = $headers["destination"] ?? $destination;
        $headers["id"] = $headers["id"] ?? generateId();

        $frame = new Frame($command, $headers, $body = "");

        try {
            yield from $this->sendFrame($frame);
        } catch (\Throwable $e) {
            $emitter->fail(new StompException(
                $msg = "Failed sending SUBSCRIBE frame to server",
                $code = 0,
                $previous = $e
            ));
            return;
        }

        $subscriptionId = $headers["id"];
        $stream = $emitter->stream();
        $this->streamIdToSubscriptionIdMap[$stream] = $subscriptionId;
        $this->subscriptionIdToEmitterMap[$subscriptionId] = $emitter;
    }

    /**
     * @return Promise<null>
     */
    public function unsubscribe(Stream $stream, array $headers = []): Promise
    {
        $streamId = \spl_object_hash($stream);
        if (!isset($this->streamIdToSubscriptionIdMap[$streamId])) {
            return new Success;
        }

        $command = Command::UNSUBSCRIBE;
        $headers["id"] = $headers["id"] ?? $subscriptionId;
        $frame = new Frame($command, $headers, $body = "");

        $promise = new Coroutine($this->sendFrame($frame));
        $promise->when(function ($e, $r) use ($streamId) {
            if (empty($e)) {
                $subscriptionId = $this->streamIdToSubscriptionIdMap[$streamId];
                $emitter = $this->subscriptionIdToEmitterMap[$subscriptionId];
                unset($this->subscriptionIdToEmitterMap[$subscriptionId]);
                $emitter->resolve();
            }
        });

        return $promise;
    }

    public function begin(string $transactionId = null, array $headers = []): Promise
    {
        return new Coroutine($this->doBegin($transactionId, $headers));
    }

    private function doBegin(string $transactionId = null, array $headers = []): \Generator
    {
        $command = Command::BEGIN;
        $transactionId = $headers["transaction"] ?? $transactionId;
        $transactionId = $transactionId ?? generateId();
        $headers["transaction"] = $transactionId;
        $frame = new Frame($command, $headers, $body = "");
        yield from $this->sendFrame($frame);

        return $transactionId;
    }

    public function commit(string $transactionId, array $headers = []): Promise
    {
        $command = Command::COMMIT;
        $headers["transaction"] = $headers["transaction"] ?? $transactionId;
        $frame = new Frame($command, $headers, $body = "");

        return new Coroutine($this->sendFrame($frame));
    }

    public function abort(string $transactionId, array $headers = []): Promise
    {
        $command = Command::ABORT;
        $headers["transaction"] = $headers["transaction"] ?? $transactionId;
        $frame = new Frame($command, $headers, $body = "");

        return new Coroutine($this->sendFrame($frame));
    }

    public function ack(string $messageId, string $transactionId = null, array $headers = []): Promise
    {
        $command = Command::ACK;

        // STOMP v1.2 requires the "id" header while previous versions expect "message-id"
        $idKey = ($this->versionInUse === Version::V_1_2) ? "id" : "message-id";
        $headers[$idKey] = $headers[$idKey] ?? $messageId;

        if (isset($headers["transaction"])) {
            $tid = $headers["transaction"];
        }
        if (isset($transactionId)) {
            $tid = $transactionId;
        }
        if (isset($tid)) {
            $headers["transaction"] = $tid;
        }

        $frame = new Frame($command, $headers, $body = "");

        return new Coroutine($this->sendFrame($frame));
    }

    public function nack(string $messageId, string $transactionId = null, array $headers = []): Promise
    {
        if ($this->versionInUse === Version::V_1_0) {
            return new Failure(new StompException(
                "NACK functionality unavailable when communicating via STOMP v1.0"
            ));
        }

        $command = Command::NACK;

        // STOMP v1.2 requires the "id" header while previous versions expect "message-id"
        $idKey = ($this->versionInUse === Version::V_1_2) ? "id" : "message-id";
        $headers[$idKey] = $headers[$idKey] ?? $messageId;

        if (isset($headers["transaction"])) {
            $tid = $headers["transaction"];
        }
        if (isset($transactionId)) {
            $tid = $transactionId;
        }
        if (isset($tid)) {
            $headers["transaction"] = $tid;
        }

        $frame = new Frame($command, $headers, $body = "");

        return new Coroutine($this->sendFrame($frame));
    }

    public function disconnect(array $headers = []): Promise
    {
        $command = Command::DISCONNECT;
        $frame = new Frame($command, $headers, $body = "");

        return new Coroutine($this->sendFrame($frame));
    }

    public function __debugInfo()
    {
        return [
            "uri" => $this->uri,
            "stream" => (string) $this->socket,
            "writeWatcher" => $this->writeWatcher,
            "readWatcher" => $this->readWatcher,
            "writeQueueSize" => \count($this->writeQueue),
            "writeBufferSize" => \strlen($this->writeBuffer),
            "readResultQueueSize" => \count($this->readResultQueue),
            "framesAwaitingReceipt" => \count($this->receiptsInWaiting),
            "lastDataSentAt" => $this->lastDataSentAt,
            "versionInUse" => Version::STRINGS[$this->versionInUse],
        ];
    }
}
