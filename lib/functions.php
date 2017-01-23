<?php

namespace Amp\Stomp;

function generateId(int $length = 32): string
{
    return \bin2hex(\random_bytes($length));
}

function parse(): \Generator
{
    start: {
        $data = "";
        $command = null;
        $headers = null;
        $body = null;
        $contentLength = null;
    }

    recv_command: {
        $data .= yield;
        goto parse_command;
    }

    parse_command: {
        $data = \ltrim($data, "\r\n");
        $eolPosition = \strpos($data, "\n");
        if ($eolPosition === false) {
            // @TODO prevent DoS (for servers) by enforcing
            // optional max header buffer size
            goto recv_command;
        }
        $command = \rtrim(\substr($data, 0, $eolPosition), "\r\n");
        $data = \substr($data, $eolPosition);

        // skip recv_headers to avoid pausing the parser until necessary
        goto standard_headers;
    }

    recv_headers: {
        $data .= yield;
        goto standard_headers;
    }

    standard_headers: {
        $headerEndPosition = \strpos($data, "\n\n");
        if ($headerEndPosition === false) {
            goto windows_headers;
        }
        $rawHeaders = \trim(\substr($data, 0, $headerEndPosition));
        $data = \substr($data, $headerEndPosition + 2);
        goto parse_headers;
    }

    windows_headers: {
        $headerEndPosition = \strpos($data, "\r\n\r\n");
        if ($headerEndPosition === false) {
            goto recv_headers;
        }
        $rawHeaders = \trim(\substr($data, 0, $headerEndPosition));
        $data = \substr($data, $headerEndPosition + 4);
        goto parse_headers;
    }

    parse_headers: {
        $headers = [];
        $headerLines = \preg_split("/[\r?\n]+/", $rawHeaders);
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
            goto parse_body_by_length;
        } else {
            goto parse_body_to_null_byte;
        }
    }

    recv_body: {
        $data .= yield;

        // @TODO prevent DoS (for servers) by enforcing
        // optional max body buffer size

        if (isset($contentLength)) {
            goto parse_body_by_length;
        } else {
            goto parse_body_to_null_byte;
        }
    }

    parse_body_by_length: {
        if (!isset($data[$contentLength])) {
            goto recv_body;
        }

        $body = \substr($data, 0, $contentLength);
        // Add to the offset because even with content-length
        // the spec requires messages to terminate with a null-byte
        $data = \substr($data, $contentLength + 1);

        goto notify;
    }

    parse_body_to_null_byte: {
        if (($nullBytePosition = \strpos($data, "\0")) === false) {
            goto recv_body;
        }

        $body = \substr($data, 0, $nullBytePosition);
        $data = \substr($data, $nullBytePosition + 1);

        goto notify;
    }

    notify: {
        $frame = new Frame($command, $headers, $body);
        $command = null;
        $headers = null;
        $body = null;
        $contentLength = null;

        $data .= yield $frame;

        if (isset($data[0])) {
            goto parse_command;
        } else {
            goto recv_command;
        }
    }
}
