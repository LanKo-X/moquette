package io.moquette.server;

import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.messages.*;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * MQTT 服务端的各种消息回调
 * Created by Lex on 11/15/16.
 */
public class MessageHandler extends AbstractInterceptHandler {
    private SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS");

    @Override
    public void onConnect(InterceptConnectMessage msg) {
        super.onConnect(msg);
        String base = String.format("Connect [clientID: %s, prot: %s, ver: %02X, clean: %b]",
                msg.getClientID(),
                msg.getProtocolName(),
                msg.getProtocolVersion(),
                msg.isCleanSession());
        if (msg.isWillFlag()) {
            base += String.format(" Will [QoS: %d, retain: %b]",
                    msg.getWillQos(), msg.isWillRetain());
        }

        println(base);
    }

    @Override
    public void onDisconnect(InterceptDisconnectMessage msg) {
        super.onDisconnect(msg);
        String base = String.format("Disconnect [clientID: %s, username: %s]",
                msg.getClientID(), msg.getUsername());
        println(base);
    }

    @Override
    public void onConnectionLost(InterceptConnectionLostMessage msg) {
        super.onConnectionLost(msg);
        String base = String.format("ConnectionLost [clientID: %s, username: %s]",
                msg.getClientID(), msg.getUsername());
        println(base);
    }

    @Override
    public void onPublish(InterceptPublishMessage msg) {
        super.onPublish(msg);
        String base = String.format("Publish [clientID: %s, username: %s, topicName: %s, payload: %s]",
                msg.getClientID(), msg.getUsername(), msg.getTopicName(), new String(msg.getPayload().array()));
        println(base);
    }

    @Override
    public void onSubscribe(InterceptSubscribeMessage msg) {
        super.onSubscribe(msg);
        String base = String.format("Subscribe [clientID: %s, username: %s, topicFilter: %s, QoS: %d]",
                msg.getClientID(), msg.getUsername(), msg.getTopicFilter(), msg.getRequestedQos().byteValue());
        println(base);
    }

    @Override
    public void onUnsubscribe(InterceptUnsubscribeMessage msg) {
        super.onUnsubscribe(msg);
        String base = String.format("Unsubscribe [clientID: %s, username: %s, topicFilter: %s]",
                msg.getClientID(), msg.getUsername(), msg.getTopicFilter());
        println(base);
    }

    @Override
    public void onMessageAcknowledged(InterceptAcknowledgedMessage msg) {
        super.onMessageAcknowledged(msg);
        String base = String.format("Acknowledged [Username: %s, topic: %s, storeMessage: %s]",
                msg.getUsername(), msg.getTopic(), msg.getMsg().toString());
        println(base);
    }

    private void println(String message) {
        Date now = new Date(System.currentTimeMillis());
        System.out.println("[" + format.format(now) + "] " + message);
    }
}
