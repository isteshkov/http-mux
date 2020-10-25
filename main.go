package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const (
	grabUrlsConcurrencyLimit = 4
	maxInputUrls             = 20
)

func main() {
	var listenAddr string

	flag.StringVar(&listenAddr, "listen-addr", ":8080", "server listen address")
	flag.Parse()

	logger := log.New(os.Stdout, "http: ", log.LstdFlags)
	server := NewHttpServer(listenAddr, logger)

	done := make(chan bool, 1)
	signalsCh := make(chan os.Signal, 1)

	signal.Notify(signalsCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-signalsCh
		logger.Println("Server is shutting down...")

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		server.SetKeepAlivesEnabled(false)
		if err := server.Shutdown(ctx); err != nil {
			logger.Fatalf("Could not gracefully shutdown the server: %v\n", err)
		}
		close(done)
	}()

	logger.Println("Server is ready to handle requests at ", server.Addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatalf("Could not listen on %s: %v\n", listenAddr, err)
	}

	<-done
	logger.Println("Server stopped")
}

func NewHttpServer(addr string, logger *log.Logger) *http.Server {

	router := http.NewServeMux()

	router.HandleFunc("/grabber", limitNumClients(func(wr http.ResponseWriter, req *http.Request) {

		if req.Method != http.MethodPost {
			wr.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if req.Header.Get("Content-Type") != "application/json" {
			responseWithError(wr, fmt.Errorf("unexpected content-type, %s provided", req.Header.Get("Content-Type")), http.StatusBadRequest)
			return
		}

		var input struct {
			Urls []string `json:"urls"`
		}

		err := json.NewDecoder(req.Body).Decode(&input)
		if err != nil {
			responseWithError(wr, fmt.Errorf("unmarshal body error: %s", err.Error()), http.StatusInternalServerError)
			return
		}

		input.Urls = removeDuplicates(input.Urls)

		if len(input.Urls) > maxInputUrls {
			responseWithError(wr, fmt.Errorf("urls count exceeded, max 20, received %d", len(input.Urls)), http.StatusBadRequest)
			return
		}

		responseChan := make(chan *GrabResponse)
		errCh := make(chan *HandleError)

		defer func() {
			close(responseChan)
			close(errCh)
		}()

		go grabData(req.Context(), responseChan, errCh, input.Urls)

		select {
		// all urls succeeded
		case response := <-responseChan:
			wr.WriteHeader(http.StatusOK)
			wr.Header().Set("Content-Type", "application/json")
			encodeErr := json.NewEncoder(wr).Encode(response)
			if encodeErr != nil {
				fmt.Printf("marshal response error, %s", encodeErr)
			}
			return
		// error occurred while handling urls
		case handleError := <-errCh:
			responseWithError(wr, handleError, handleError.StatusCode)
			return
		// client gave up
		case <-req.Context().Done():
			return
		}
	}, 100))

	appHandler := accessLog(router)
	appHandler = recovery(appHandler)

	return &http.Server{
		Addr:         addr,
		Handler:      appHandler,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 20 * time.Second,
		IdleTimeout:  10 * time.Second,
		ErrorLog:     logger,
	}
}

// recovery is a middleware that handles a panic
func recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		defer func() {
			err := recover()
			if err != nil {
				// log error, send it to sentry, etc.
				fmt.Println(err)

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				err = json.NewEncoder(w).Encode(map[string]string{
					"error": "internal_server_error",
				})
				if err != nil {
					fmt.Printf("marshal response error, %s", err)
				}
			}

		}()

		next.ServeHTTP(w, r)

	})
}

// accessLog is a middleware that logs http requests info
func accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		defer func(start time.Time) {
			fmt.Printf("[%s] %s, %s %s\n", r.Method, r.RemoteAddr, r.URL.Path, time.Since(start))
		}(time.Now())

		next.ServeHTTP(w, r)

	})
}

// limitNumClients is HTTP handling middleware that ensures no more than
// maxClients requests are passed concurrently to the given handler f.
func limitNumClients(f http.HandlerFunc, maxClients int) http.HandlerFunc {
	// Counting semaphore using a buffered channel
	sema := make(chan struct{}, maxClients)

	return func(w http.ResponseWriter, req *http.Request) {
		sema <- struct{}{}
		defer func() { <-sema }()
		f(w, req)
	}
}

func responseWithError(wr http.ResponseWriter, err error, status int) {
	wr.WriteHeader(status)
	wr.Header().Set("Content-Type", "application/json")
	encodeErr := json.NewEncoder(wr).Encode(struct {
		Error string `json:"error"`
	}{
		Error: err.Error(),
	})
	if encodeErr != nil {
		fmt.Printf("marshal response error, %s", encodeErr)
	}
	return
}

type HandleError struct {
	StatusCode  int    `json:"-"`
	ErrorCode   string `json:"error_code"`
	Description string `json:"description"`
}

func (he *HandleError) Error() string {
	return fmt.Sprintf("%s", he.Description)
}

type GrabResponse struct {
	Results map[string]UrlOutput `json:"results"`
}

type UrlOutput struct {
	Status        string `json:"status"`
	ContentLength int64  `json:"content_length"`
	Body          string `json:"body"`
}

func grabData(ctx context.Context, responseChan chan *GrabResponse, errChan chan *HandleError, urls []string) {

	var response GrabResponse
	response.Results = make(map[string]UrlOutput)

	ctxConcurrent, cancel := context.WithCancel(ctx)
	defer cancel()

	// this buffered channel will block at the concurrency limit
	semaphoreChan := make(chan struct{}, grabUrlsConcurrencyLimit)

	// this channel will not block and collect the http request results
	resultsChan := make(chan *Result)

	// make sure we close these channels when we're done with them
	defer func() {
		close(semaphoreChan)
		close(resultsChan)
	}()

	for i := range urls {
		go func(ctx context.Context, url string) {
			select {
			case semaphoreChan <- struct{}{}:

				timeoutCtx, _ := context.WithTimeout(ctx, 1*time.Second)

				req, err := http.NewRequestWithContext(timeoutCtx, "GET", url, nil)
				if err != nil {
					resultsChan <- &Result{url, nil, err}
					return
				}

				res, err := http.DefaultClient.Do(req)
				if err != nil {
					if !strings.Contains(err.Error(), context.Canceled.Error()) {
						resultsChan <- &Result{url, nil, err}
					}
					return
				}
				defer res.Body.Close()

				bodyBytes, err := ioutil.ReadAll(res.Body)
				if err != nil {
					log.Fatal(err)
				}

				result := &Result{url, &UrlOutput{
					Status:        res.Status,
					ContentLength: res.ContentLength,
					Body:          string(bodyBytes),
				}, err}

				resultsChan <- result

				<-semaphoreChan
			case <-ctx.Done():
				return
			}
		}(ctxConcurrent, urls[i])
	}

	// start listening for any results over the resultsCh
	// once we get a result put it to the result map
	for result := range resultsChan {
		if result.Err != nil {
			if !strings.Contains(result.Err.Error(), context.Canceled.Error()) {
				cancel()
				errChan <- &HandleError{
					StatusCode:  http.StatusInternalServerError,
					ErrorCode:   "urls_handle_error",
					Description: fmt.Sprintf("there is an error while handling one of provided urls - %s", result.Url),
				}
			}
			return
		}

		if result.Res != nil {
			response.Results[result.Url] = *result.Res
		}

		if len(response.Results) == len(urls) {
			break
		}
	}

	responseChan <- &response
	return
}

// a struct to hold the result from each request including an index
// which will be used for sorting the results after they come in
type Result struct {
	Url string
	Res *UrlOutput
	Err error
}

func removeDuplicates(list []string) []string {

	uniques := make(map[string]struct{})
	var uniqueList []string

	for i := range list {
		if _, ok := uniques[list[i]]; !ok {
			uniques[list[i]] = struct{}{}
			uniqueList = append(uniqueList, list[i])
		}
	}

	return uniqueList
}