<?php

namespace Amp\Stomp;

class Frame
{
    private $command;
    private $headers;
    private $body;

    public function __construct(string $command, array $headers, string $body)
    {
        $this->command = $command;
        $this->headers = $headers;
        $this->body = $body;
    }

    public function getCommand(): string
    {
        return $this->command;
    }

    public function getHeaders(): array
    {
        return $this->headers;
    }

    public function getBody(): string
    {
        return $this->body;
    }

    public function list(): array
    {
        return [$this->command, $this->headers, $this->body];
    }

    public function __toString(): string
    {
        $headers = "";
        foreach ($this->headers as $key => $value) {
            $headers .= "{$key}:{$value}\n";
        }
        return \implode("\n", [
            $this->command,
            $headers,
            $this->body . "\0"
        ]); 
    }
}
