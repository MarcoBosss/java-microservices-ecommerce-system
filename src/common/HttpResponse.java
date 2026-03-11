package common;

public class HttpResponse {
    public int status;
    public String body;

    public HttpResponse(int status, String body) {
        this.status = status;
        this.body = body;
    }
}