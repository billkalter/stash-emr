package com.bazaarvoice.emodb.stash.emr.databus;

public class PollFailedException extends RuntimeException {

    private final int _responseCode;
    private final String _content;

    public PollFailedException(String subscription, int responseCode, String content) {
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
