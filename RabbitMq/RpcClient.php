<?php

namespace OldSound\RabbitMqBundle\RabbitMq;

use OldSound\RabbitMqBundle\RabbitMq\BaseAmqp;
use PhpAmqpLib\Message\AMQPMessage;

class RpcClient extends BaseAmqp
{
    protected $requests = 0;
    protected $replies = array();
    protected $queueName;
    protected $timeout = 0;

    public function initClient()
    {
        list($this->queueName, ,) = $this->getChannel()->queue_declare("", false, false, true, true);
    }

    public function addRequest($msgBody, $server, $requestId = null, $routingKey = '')
    {
        if (empty($requestId)) {
            throw new \InvalidArgumentException('You must provide a $requestId');
        }

        $msg = new AMQPMessage($msgBody, array('content_type' => 'text/plain',
                                               'reply_to' => $this->queueName,
                                               'correlation_id' => $requestId));

        $this->getChannel()->basic_publish($msg, $server, $routingKey);

        $this->requests++;
		
        if (isset($server['expiration']) && $server['expiration'] > $this->timeout) {
            $this->timeout = $server['expiration'];
        }
    }

    public function getReplies()
    {
        $this->replies = array();
        $this->getChannel()->basic_consume($this->queueName, '', false, true, false, false, array($this, 'processMessage'));

        while (count($this->replies) < $this->requests) {
            $this->getChannel()->wait(null, false, $this->timeout);
        }

        $this->getChannel()->basic_cancel($this->queueName);
        $this->requests = 0;
        $this->timeout = 0;

        return $this->replies;
    }

    public function processMessage(AMQPMessage $msg)
    {
        $this->replies[$msg->get('correlation_id')] = unserialize($msg->body);
    }
}
