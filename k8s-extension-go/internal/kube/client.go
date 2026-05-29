package kube

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
)

type Client struct {
	baseURL   string
	namespace string
	token     string
	http      *http.Client
}

func NewInCluster(namespace string) (*Client, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("KUBERNETES_SERVICE_HOST/PORT are required")
	}
	tokenRaw, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return nil, err
	}
	caRaw, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caRaw)
	return &Client{
		baseURL:   "https://" + host + ":" + port,
		namespace: namespace,
		token:     string(tokenRaw),
		http: &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		}},
	}, nil
}

func (c *Client) Namespace() string { return c.namespace }

func (c *Client) Get(apiPath string, out any) (int, error) {
	return c.do(http.MethodGet, apiPath, nil, out)
}

func (c *Client) Create(apiPath string, body any, out any) (int, error) {
	return c.do(http.MethodPost, apiPath, body, out)
}

func (c *Client) Put(apiPath string, body any, out any) (int, error) {
	return c.do(http.MethodPut, apiPath, body, out)
}

func (c *Client) PatchMerge(apiPath string, body any, out any) (int, error) {
	return c.doWithContentType(http.MethodPatch, apiPath, body, out, "application/merge-patch+json")
}

func (c *Client) Delete(apiPath string) (int, error) {
	return c.do(http.MethodDelete, apiPath, nil, nil)
}

func (c *Client) Watch(ctx context.Context, apiPath, resourceVersion string, onEvent func(eventType string, object map[string]any)) error {
	sep := "?"
	if strings.Contains(apiPath, "?") {
		sep = "&"
	}
	watchURL := c.baseURL + apiPath + sep + "watch=true&allowWatchBookmarks=true&timeoutSeconds=300"
	if resourceVersion != "" {
		watchURL += "&resourceVersion=" + url.QueryEscape(resourceVersion)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, watchURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("authorization", "Bearer "+c.token)
	watchClient := &http.Client{Transport: c.http.Transport}
	resp, err := watchClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("watch %s returned %d: %s", apiPath, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	dec := json.NewDecoder(resp.Body)
	for {
		var event struct {
			Type   string         `json:"type"`
			Object map[string]any `json:"object"`
		}
		if err := dec.Decode(&event); err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		onEvent(event.Type, event.Object)
	}
}

func (c *Client) Upsert(apiPath string, body map[string]any, out any) error {
	status, err := c.Get(apiPath, nil)
	if err != nil && status != http.StatusNotFound {
		return err
	}
	if status == http.StatusNotFound {
		status, err = c.Create(parentPath(apiPath), body, out)
	} else {
		status, err = c.PatchMerge(apiPath, body, out)
	}
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("upsert %s returned %d", apiPath, status)
	}
	return nil
}

func (c *Client) do(method, apiPath string, body any, out any) (int, error) {
	return c.doWithContentType(method, apiPath, body, out, "application/json")
}

func (c *Client) doWithContentType(method, apiPath string, body any, out any, contentType string) (int, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, c.baseURL+apiPath, reader)
	if err != nil {
		return 0, err
	}
	req.Header.Set("authorization", "Bearer "+c.token)
	if body != nil {
		req.Header.Set("content-type", contentType)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if out != nil && len(raw) > 0 && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, err
		}
	}
	if resp.StatusCode >= 400 {
		return resp.StatusCode, fmt.Errorf("%s %s returned %d: %s", method, apiPath, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return resp.StatusCode, nil
}

func parentPath(apiPath string) string {
	return path.Dir(apiPath)
}

func NamespacedResource(ns, plural string) string {
	return "/apis/mig.or-sim.io/v1alpha1/namespaces/" + ns + "/" + plural
}

func NamespacedResourceName(ns, plural, name string) string {
	return NamespacedResource(ns, plural) + "/" + name
}

func Deployment(ns, name string) string {
	return "/apis/apps/v1/namespaces/" + ns + "/deployments/" + name
}

func Deployments(ns string) string {
	return "/apis/apps/v1/namespaces/" + ns + "/deployments"
}

func Pods(ns string) string {
	return "/api/v1/namespaces/" + ns + "/pods"
}

func Service(ns, name string) string {
	return "/api/v1/namespaces/" + ns + "/services/" + name
}

func Nodes() string { return "/api/v1/nodes" }

func Node(name string) string { return "/api/v1/nodes/" + name }
