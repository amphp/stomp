<?php

namespace Amp\Stomp\Test;

use function Amp\Stomp\parse;

class FunctionsTest extends \PHPUnit_Framework_TestCase
{
    public function testBasicParse()
    {
        $lines[] = "SEND";
        $lines[] = "destination:/queue/a";
        $lines[] = "content-type:text/plain";
        $lines[] = "";
        $lines[] = "hello queue a\0";

        $msg = \implode("\n", $lines);

        $parser = parse();
        $frame = $parser->send($msg);
        
        $expectedCommand = "SEND";
        $expectedHeaders = [
            "destination" => "/queue/a",
            "content-type" => "text/plain"
        ];
        $expectedBody = "hello queue a";
        
        $this->assertSame($expectedCommand, $frame->getCommand());
        $this->assertSame($expectedHeaders, $frame->getHeaders());
        $this->assertSame($expectedBody, $frame->getBody());
    }

    public function testBasicParseWithWindowsLineEndings()
    {
        $msg = "SEND\r\n";
        $msg .= "destination:/queue/a\r\n";
        $msg .= "content-type:text/plain\r\n";
        $msg .= "\r\n";
        $msg .= "hello queue a\0";

        $parser = parse();
        $frame = $parser->send($msg);
        
        $expectedCommand = "SEND";
        $expectedHeaders = [
            "destination" => "/queue/a",
            "content-type" => "text/plain"
        ];
        $expectedBody = "hello queue a";
        
        $this->assertSame($expectedCommand, $frame->getCommand());
        $this->assertSame($expectedHeaders, $frame->getHeaders());
        $this->assertSame($expectedBody, $frame->getBody());
    }

    public function testIncrementalParse()
    {
        $lines[] = "SEND";
        $lines[] = "destination:/queue/a";
        $lines[] = "content-type:text/plain";
        $lines[] = "";
        $lines[] = "hello queue a\0";

        $msg = \implode("\n", $lines);

        $parser = parse();

        for ($i=0; $i < \strlen($msg); $i++) {
            $frame = $parser->send($msg[$i]);

            if ($i === \strlen($msg) - 1) {
                $this->assertInstanceOf("Amp\\Stomp\\Frame", $frame);
            } else {
                $this->assertNull($frame);
            }
        }
        
        $expectedCommand = "SEND";
        $expectedHeaders = [
            "destination" => "/queue/a",
            "content-type" => "text/plain"
        ];
        $expectedBody = "hello queue a";
        
        $this->assertSame($expectedCommand, $frame->getCommand());
        $this->assertSame($expectedHeaders, $frame->getHeaders());
        $this->assertSame($expectedBody, $frame->getBody());
    }

    public function testBasicParseWithContentLength()
    {
        $lines[] = "SEND";
        $lines[] = "destination:/queue/a";
        $lines[] = "content-type:text/plain";
        $lines[] = "content-length:13";
        $lines[] = "";
        $lines[] = "hello queue a\0";

        $msg = \implode("\n", $lines);

        $parser = parse();

        for ($i=0; $i < \strlen($msg); $i++) {
            $frame = $parser->send($msg[$i]);

            if ($i === \strlen($msg) - 1) {
                $this->assertInstanceOf("Amp\\Stomp\\Frame", $frame);
            } else {
                $this->assertNull($frame);
            }
        }
        
        $expectedCommand = "SEND";
        $expectedHeaders = [
            "destination" => "/queue/a",
            "content-type" => "text/plain",
            "content-length" => "13"
        ];
        $expectedBody = "hello queue a";
        
        $this->assertSame($expectedCommand, $frame->getCommand());
        $this->assertSame($expectedHeaders, $frame->getHeaders());
        $this->assertSame($expectedBody, $frame->getBody());
    }

}
