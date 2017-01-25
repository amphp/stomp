<?php

namespace Amp\Stomp;

use Amp\Promise;
use Amp\Success;
use Amp\Failure;
use Amp\Deferred;

use function Amp\resolve;
use function Amp\cancel;
use function Amp\enable;
use function Amp\disable;
use function Amp\immediately;
use function Amp\repeat;
use function Amp\onReadable;
use function Amp\onWritable;

class Client
{
    const OPTIONS = [
        "default_content_type"  => 0,
        "require_receipt"       => 1,
        "max_queued_writes"     => 2,
        "force_connect_frame"   => 3,
        "vhost"                 => 4,
        "login"                 => 5,
        "passcode"              => 6,
        "accept_version"        => 7,
        "heart_beat_enable"     => 8,
        "heart_beat_min"        => 9,
        "heart_beat_max"        => 10,
        "universal_headers"     => 11,
    ];

    const ALLOWED_ACKS = [
        "auto" => "auto",
        "client" => "client",
        "client-individual" => "client-individual",
    ];

    const DEFAULT_PORT = "61613";

    private $uri;
    private $parser;
    private $stream;
    private $readWatcher;
    private $writeWatcher;
    private $writeQueue = [];
    private $writeBuffer = "";
    private $writeDeferred;
    private $readDeferred;
    private $readResultQueue = [];
    private $receiptsInWaiting = [];
    private $connectionPromise;
    private $filter;
    private $heartbeatWatcher;
    private $lastDataSentAt;
    private $versionInUse;

    private $options = [
        self::OPTIONS["default_content_type"]   => "text/plain",
        self::OPTIONS["require_receipt"]        => false,
        self::OPTIONS["max_queued_writes"]      => 2048,
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
    }

    private function setOptions(array $options)
    {
        // @TODO actually validate and assign these
        foreach ($options as $key => $value) {
            $this->options[$key] = $value;
        }
    }

    private function isConnected(): bool
    {
        return $this->stream && \is_resource($this->stream) && !\feof($this->stream);
    }

    /**
     * @return Promise<null>
     */
    public function connect(): Promise
    {
        if ($this->stream) {
            return new Success;
        }
        if ($this->connectionPromise) {
            return $this->connectionPromise;
        }

        $connectionPromise = resolve($this->doConnect());
        $connectionPromise->when(function ($e) {
            if ($e) {
                $this->cleanup();
            }
        });

        return $this->connectionPromise = $connectionPromise;
    }

    private function doConnect(): \Generator
    {
        $this->parser = parse();
        $this->stream = yield \Amp\Socket\connect($this->uri);
        $this->writeWatcher = onWritable($this->stream, function () {
            $this->doStreamWrite();
        });
        \Amp\disable($this->writeWatcher);
        $this->readWatcher = onReadable($this->stream, function () {
            $this->consume();
        });

        $command = $this->buildConnectCommand();
        $headers = $this->buildConnectHeaders();
        $frame = new Frame($command, $headers, $body = "");

        yield from $this->sendFrame($frame);

        $frame = yield $this->read();
        
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

        $this->connectionPromise = null;
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
        $this->heartbeatWatcher = repeat(function () use ($max) {
            $this->doHeartbeat($max); 
        }, 1000);
    }

    private function doHeartbeat(int $interval)
    {
        // if data is queued for write there's no point in sending a heartbeat
        if ($this->writeQueue || isset($this->writeBuffer[0])) {
            return;
        }

        // if the heartbeat timout has yet to elapse we don't need to send
        $now = \microtime(true);
        $msSinceLastSend = (($now - $this->lastDataSentAt) * 1000);
        if ($msSinceLastSend < ($interval - 1000)) {
            return;
        }

        $this->writeBuffer = "\n";
        $this->writeDeferred = null;
        $this->doStreamWrite();
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

        yield from $this->doWrite($command, $headers, $body);
    }

    private function cleanup()
    {
        if (isset($this->writeWatcher)) {
            \Amp\cancel($this->writeWatcher);
        }
        if (isset($this->readWatcher)) {
            \Amp\cancel($this->readWatcher);
        }
        if (isset($this->heartbeatWatcher)) {
            \Amp\cancel($this->heartbeatWatcher);
        }

        $this->writeWatcher = null;
        $this->readWatcher = null;
        $this->heartbeatWatcher = null;
        $this->stream = null;
        $this->connectionPromise = null;

        if ($this->readDeferred) {
            $this->readDeferred->fail(new StompException("Connection gone"));
        }
    }

    private function doWrite(string $command, array $headers, string $body): \Generator
    {
        yield $this->connect();

        $maxQueuedWrites = $this->options[self::OPTIONS["max_queued_writes"]];
        if (isset($this->writeQueue[$maxQueuedWrites - 1])) {
            throw new StompException(
                "Cannot write frame; max_queued_writes limit reached ({$maxQueuedWrites})"
            );
        }

        $normalizedHeaders = $this->normalizeHeaders($headers);
        $dataToWrite = "{$command}\n{$normalizedHeaders}\n{$body}\0";

        yield $this->streamWrite($dataToWrite);

        if (isset($normalizedHeaders["receipt"])) {
            $deferred = new Deferred;
            $this->receiptsInWaiting[$normalizedHeaders["receipt"]] = $deferred;

            yield $deferred->promise();
        }
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

    private function streamWrite(string $dataToWrite): Promise
    {
        $deferred = new Deferred;

        if ($this->writeDeferred || isset($this->writeBuffer[0])) {
            $this->writeQueue[] = [$dataToWrite, $deferred];
        } else {
            $this->writeBuffer = $dataToWrite;
            $this->writeDeferred = $deferred;
            $this->doStreamWrite();
        }

        return $deferred->promise();
    }

    private function doStreamWrite()
    {
        $bytesWritten = @\fwrite($this->stream, $this->writeBuffer);
        if (empty($bytesWritten)) {
            $this->onEmptyWrite();
        } elseif (isset($this->writeBuffer[$bytesWritten])) {
            $this->writeBuffer = \substr($this->writeBuffer, $bytesWritten);
            enable($this->writeWatcher);
        } else {
            $this->lastDataSentAt = \microtime(true);
            $this->onCompleteWrite();
        }
    }

    private function onEmptyWrite()
    {
        if (!$this->isConnected()) {
            $this->cleanup();
            $this->writeDeferred->fail(new StompException(
                "Connection went away :("
            ));
        }
    }

    private function onCompleteWrite()
    {
        // Allow empty deferreds so it's possible to insert heart-beat sends
        if ($oldDeferred = $this->writeDeferred) {
            immediately([$oldDeferred, "succeed"]);
        }
        if ($this->writeQueue) {
            list($this->writeBuffer, $this->writeDeferred) = \array_shift($this->writeQueue);
            enable($this->writeWatcher);
        } else {
            $this->writeDeferred = null;
            $this->writeBuffer = "";
            disable($this->writeWatcher);
        }
    }

    /**
     * @return Promise<Frame>
     */
    public function read(): Promise
    {
        if ($this->readResultQueue) {
            $frame = \array_shift($this->readResultQueue);
            return new Success($frame);
        }
        if (empty($this->readDeferred)) {
            $this->readDeferred = new Deferred;
        }

        return $this->readDeferred->promise();
    }

    private function consume()
    {
        $data = @\fread($this->stream, 8192);
        $hasData = isset($data[0]);
        if (!($hasData || $this->isConnected())) {
            $this->cleanup();
        }
        if (!$hasData) {
            return;
        }
        if (!$frame = $this->parser->send($data)) {
            return;
        }

        $frame = $this->filter->filter(Filter::READ, $frame);

        switch ($frame->getCommand()) {
            case Command::CONNECTED:
            case Command::MESSAGE:
                break;
            case Command::RECEIPT:
                $this->onServerReceiptFrame($frame);
                return;
            case Command::ERROR:
                $this->onServerErrorFrame($frame);
                return;
        }

        if ($deferred = $this->readDeferred) {
            $this->readDeferred = null;
            $deferred->succeed($frame);
            return;
        }

        $this->readResultQueue[] = $frame;
    }

    private function onServerReceiptFrame(Frame $frame)
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
        immediately(static function () use ($toSucceed) {
            foreach ($toSucceed as $deferred) {
                $deferred->succeed();
            }
        });
    }

    private function onServerErrorFrame(Frame $frame)
    {
        list($command, $headers, $body) = $frame->list();
        $e = new StompException(
            "ERROR frame received:\n\n{$frame}"
        );

        immediately(function () use ($e) {
            $this->failOutstandingPromises($e);
        });
    }

    private function failOutstandingPromises(\Throwable $e)
    {
        if ($this->readDeferred) {
            $this->readDeferred->fail($e);
            $this->readDeferred = null;
        }
        if (empty($this->receiptsInWaiting)) {
            return;
        }
        $receiptError = new StompException(
            $msg = "Error experienced while awaiting RECEIPT",
            $code = 0,
            $prev = $e
        );
        $receiptsToFail = $this->receiptsInWaiting;
        $this->receiptsInWaiting = [];
        foreach ($receiptsToFail as $deferred) {
            $deferred->fail($receiptError);
        }
    }

    /**
     *
     */
    public function send(string $destination, string $data, array $headers = []): Promise
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

        return resolve($this->sendFrame($frame));
    }

    /**
     * @return Promise<string> Resolves to subscription identifier string
     */
    public function subscribe(string $destination, array $headers = []): Promise
    {
        return resolve($this->doSubscribe($destination, $headers));
    }

    private function doSubscribe(string $destination, array $headers): \Generator
    {
        $ack = $headers["ack"] ?? "auto";
        if (!isset(self::ALLOWED_ACKS[$ack])) {
            throw new StompException(
                "Invalid ack header value: {$ack}"
            );
        }
        if ($this->versionInUse === Version::V_1_0 &&
            $ack === self::ALLOWED_ACKS["client-individual"]
        ) {
            throw new StompException(
                "STOMP v1.0 does not support the use of ack:client-individual ... " .
                "Please use ack:auto or ack:client instead."
            );
        }

        $command = Command::SUBSCRIBE;
        $headers = \array_change_key_case($headers, \CASE_LOWER);
        $headers["destination"] = $headers["destination"] ?? $destination;
        $headers["id"] = $headers["id"] ?? generateId();

        $frame = new Frame($command, $headers, $body = "");
        yield from $this->sendFrame($frame);

        return (string) $headers["id"];
    }

    /**
     * @return Promise<null>
     */
    public function unsubscribe(string $subscriptionId, array $headers = []): Promise
    {
        $command = Command::UNSUBSCRIBE;
        $headers["id"] = $headers["id"] ?? $subscriptionId;
        $frame = new Frame($command, $headers, $body = "");

        return resolve($this->sendFrame($frame));
    }

    public function begin(string $transactionId = null, array $headers = []): Promise
    {
        return resolve($this->doBegin($transactionId, $headers));
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

        return resolve($this->sendFrame($frame));
    }

    public function abort(string $transactionId, array $headers = []): Promise
    {
        $command = Command::ABORT;
        $headers["transaction"] = $headers["transaction"] ?? $transactionId;
        $frame = new Frame($command, $headers, $body = "");

        return resolve($this->sendFrame($frame));
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

        return resolve($this->sendFrame($frame));
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

        return resolve($this->sendFrame($frame));
    }

    public function disconnect(array $headers = []): Promise
    {
        $command = Command::DISCONNECT;
        $frame = new Frame($command, $headers, $body = "");

        return resolve($this->sendFrame($frame));
    }

    public function __debugInfo()
    {
        return [
            "uri" => $this->uri,
            "stream" => (string) $this->stream,
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
