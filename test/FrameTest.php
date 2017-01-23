<?php

namespace Amp\Stomp\Test;

use Amp\Stomp\Frame;

class FrameTest extends \PHPUnit_Framework_TestCase
{
    public function testGetCommand()
    {
        $frame = new Frame("MESSAGE", [], "body");
        $this->assertSame("MESSAGE", $frame->getCommand());
    }

    public function testGetHeaders()
    {
        $frame = new Frame("MESSAGE", [], "body");
        $this->assertSame([], $frame->getHeaders());
    }

    public function testGetBody()
    {
        $frame = new Frame("MESSAGE", [], "body");
        $this->assertSame("body", $frame->getBody());
    }

    public function testList()
    {
        $frame = new Frame("MESSAGE", [], "body");
        $this->assertSame(["MESSAGE", [], "body"], $frame->list());
    }

    public function testToString()
    {
        $headers = ["header-foo" => "bar", "header-baz" => "bat"];
        $expected = "SEND\nheader-foo:bar\nheader-baz:bat\n\nbody\0";

        $frame = new Frame("SEND", $headers, "body");
        $this->assertSame($expected, (string) $frame);
    }
}
