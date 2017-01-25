<?php

namespace Amp\Stomp;

function generateId(int $length = 32): string
{
    return \bin2hex(\random_bytes($length));
}

function parse(): \Generator
{
    $data = yield;

    while (true) {
        $data = \ltrim($data, "\r\n");
        while ($data == "") {
            $data = \ltrim(yield, "\r\n");
        }

        while (($eolPosition = \strpos($data, "\n")) === false) {
            // @TODO prevent DoS (for servers) by enforcing
            // optional max header buffer size
            $data .= yield;
        }

        $command = \rtrim(\substr($data, 0, $eolPosition), "\r\n");
        $data = \substr($data, $eolPosition);

        while (($rawHeaders = \strstr($data, "\n\n", true)) === false && ($rawHeaders = \strstr($data, "\r\n\r\n", true)) === false) {
            $data .= yield;
        }

        $data = \substr($data, \strlen($rawHeaders) + ($data[\strlen($rawHeaders)] === "\n" ? 2 : 4));

        $headers = [];
        $headerLines = \preg_split("/[\r?\n]+/", $rawHeaders, -1, PREG_SPLIT_NO_EMPTY);
        foreach ($headerLines as $headerLine) {
            $headerDetails = \explode(':', $headerLine, 2);
            $field = $headerDetails[0];
            $value = $headerDetails[1] ?? "";
            // Only the first occurrence of a header matters in STOMP
            if (!isset($headers[$field])) {
                $headers[$field] = $value;
            }
        }

        if (isset($headers["content-length"])) {
            $contentLength = (int) $headers["content-length"];
            while (!isset($data[$contentLength])) {
                $data .= yield;
            }
            // Note that even with content-length the spec requires messages to terminate with a null-byte
        } else {
            while (($contentLength = strpos($data, "\0")) === false) {
                $data .= yield;
            }
        }

        $body = \substr($data, 0, $contentLength);
        $data = \substr($data, $contentLength + 1);

        $frame = new Frame($command, $headers, $body);
        $data .= yield $frame;
    }
}
