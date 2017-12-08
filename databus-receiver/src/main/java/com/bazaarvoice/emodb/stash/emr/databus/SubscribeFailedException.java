package com.bazaarvoice.emodb.stash.emr.databus;

public class SubscribeFailedException extends RuntimeException {

    private final int _responseCode;
    private final String _content;

    public SubscribeFailedException(String subscription, int responseCode, String content) {
        super("Subscribe request failed for subscription \"" + subscription + "\"");
        _responseCode = responseCode;
        _content = content;
    }

    public int getResponseCode() {
        return _responseCode;
    }

    public String getContent() {
        return _content;
    }
}
