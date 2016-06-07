import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryRequest;
import java.util.Iterator;
import java.io.IOException;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
/**
 * Class that pages through a BigQuery Request.
 */
class PageIterator implements Iterator<GetQueryResultsResponse> {
	private BigqueryRequest<GetQueryResultsResponse> request;
    private boolean hasNext = true;
    /**
     * Class that represents our iterator to page through results.
     * @param requestTemplate The object that represents the call to fetch
     *                          the results.
     */
    public PageIterator(final BigqueryRequest<GetQueryResultsResponse> requestTemplate) {
        this.request = requestTemplate;
    }
    /**
     * Checks whether there is another page of results.
     * @return True if there is another page of results.
     */
    public boolean hasNext() {
        return hasNext;
    }

    /**
     * Returns the next page of results.
     * @return The next page of resul.ts
     */
    public GetQueryResultsResponse next() {
        if (!hasNext) {
          throw new NoSuchElementException();
        }
        try {
          GetQueryResultsResponse response = request.execute();
          if (response.containsKey("pageToken")) {
            request = request.set("pageToken", response.get("pageToken"));
          } else {
            hasNext = false;
          }
          return response;
        } catch (IOException e) {
          e.printStackTrace();
          return null;
        }
    }
    /**
      * Skips the page by moving the iterator to the next page.
      */
    public void remove() {
        this.next();
    }
} 