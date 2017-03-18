<?php

namespace Amp\Stomp;

use Amp\Loop;
use Amp\Promise;
use Amp\Struct;
use Amp\Iterator;
use Amp\Emitter;
use Amp\Listener;
use Amp\Deferred;

/**
 * @internal
 */
class FrameStream implements Iterator
{
    private $state;

    public function __construct($socket)
    {
        $this->state = $state = new class
        {
            use Struct;

            public $socket;
            public $parser;
            public $emitter;
            public $stream;
            public $listener;
            public $writeBuffer;
            public $writeDeferred;
            public $writeQueue;
            public $readWatcherId;
            public $writeWatcherId;
        };

        $state->socket = $socket;
        $state->parser = parse();
        $state->emitter = new Emitter;
        $state->stream = $state->emitter->stream();
        $state->listener = new Listener($state->stream);
        $onReadable = static function () use ($state) {
            self::onReadable($state);
        };
        $onWritable = static function () use ($state) {
            self::onWritable($state);
        };
        $state->readWatcherId = Loop::onReadable($socket, $onReadable);
        $state->writeWatcherId = Loop::onWritable($socket, $onWritable);
        Loop::disable($state->writeWatcherId);
    }

    public function __destruct()
    {
        \fclose($this->state->socket);
        Loop::cancel($this->state->readWatcherId);
        Loop::cancel($this->state->writeWatcherId);
    }

    public function advance(): Promise
    {
        return $this->state->listener->advance();
    }

    public function getCurrent()
    {
        return $this->state->listener->getCurrent();
    }

    public function getResult()
    {
        return $this->state->listener->getResult();
    }

    public function drain(): array
    {
        return $this->state->listener->drain();
    }

    public function throttle(Promise $until)
    {
        Loop::disable($this->state->readWatcherId);
        $until->when(static function () {
            Loop::enable($this->state->readWatcherId);
        });
    }

    public function send(string $dataToSend): Promise
    {
        $state = $this->state;
        $writeDeferred = new Deferred;

        if ($state->writeDeferred) {
            $state->writeQueue[] = [$dataToSend, $writeDeferred];
            return $writeDeferred->promise();
        }

        $state->writeBuffer = $dataToSend;
        $state->writeDeferred = $writeDeferred;

        self::onWritable($state);

        return $writeDeferred->promise();
    }

    private static function onReadable($state)
    {
        $data = @\fread($state->socket, 8192);
        if (!isset($data[0]) && !\is_resource($state->socket) && !\feof($state->socket)) {
            $state->emitter->fail(new StompException("Connection gone"));
            return;
        }
        if ($frame = $state->parser->send($data)) {
            $state->emitter->emit($frame);
        }
    }

    private static function onWritable($state)
    {
        $bytesWritten = @\fwrite($state->socket, $state->writeBuffer);

        if ($bytesWritten && !isset($state->writeBuffer[$bytesWritten])) {
            $deferred = $state->writeDeferred;
            if (empty($state->writeQueue)) {
                $state->writeBuffer = "";
                $state->writeDeferred = null;
                Loop::disable($state->writeWatcherId);
            } else {
                list($state->writeBuffer, $state->writeDeferred) = \array_shift($state->writeQueue);
            }
            $deferred->resolve();
            return;
        }
        if ($bytesWritten) {
            $state->writeBuffer = \substr($state->writeBuffer, $bytesWritten);
            return;
        }
        if (\is_resource($state->socket) && !\feof($state->socket)) {
            return;
        }

        $e = new StompException("Connection gone");
        Loop::cancel($state->readWatcherId);
        Loop::cancel($state->writeWatcherId);

        $state->emitter->fail($e);
        $state->writeDeferred->fail($e);
        $writeQueue = $state->writeQueue;
        $state->writeQueue = [];
        foreach ($writeQueue as list(, $deferred)) {
            $deferred->fail($e);
        }
    }
}
