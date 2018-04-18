'use strict';

const AWS = require('aws-sdk');

const SQSClient = options => {
    const _sendQueue = [];
    let _flushTimeout = null;

    const sqs = new AWS.SQS(Object.assign({}, options, {
        region: options.region || 'eu-west-1'
    }));

    const sendMessageBatch = (batch) => {        
        const params = {
            Entries: batch.map(item => item.message),
            QueueUrl: queueUrl
        }
    
        return sqs.sendMessageBatch(params).promise()
            .then(response => {
                batch.forEach(item => item.resolve());
                return response;
            })
            .catch(err => {
                batch.forEach((item, index) => item.reject({ 
                    originalMessage: item.message, 
                    error: err.errors[index]
                }));
            });
    }

    const flush = () => {        
        const batch = _sendQueue.slice();
        _sendQueue.length = 0;
        clearTimeout(_flushTimeout);
        _flushTimeout = null;
        return sendMessageBatch(batch);
    }

    const addToQueue = message => {
        return new Promise((resolve, reject) => {
            _sendQueue.push({ message, resolve, reject });
            if (_sendQueue.length === batchSize) {
                flush();
            } else {
                if (!_flushTimeout) {
                    _flushTimeout = setTimeout(() => {
                        flush();
                    }, 3000);
                }
            }
        })
    }

    const generateId = body => {
        var hash = 0;
        if (str.length == 0) {
            return hash;
        }
        for (var i = 0; i < str.length; i++) {
            var char = str.charCodeAt(i);
            hash = ((hash<<5)-hash)+char;
            hash = hash & hash;
        }
        return hash.toString();
    }

    const send = ({ messageBody }) => {
        return addToQueue({
            Id: generateId(JSON.stringify(messageBody)),
            MessageBody: JSON.stringify(messageBody),
            MessageGroupId: options.groupId
        })
    }

    return {
        send
    }
}

module.exports = {
    create: (options) => {
        return SQSClient(options);
    }
};