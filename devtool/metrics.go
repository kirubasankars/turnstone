// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package devtool

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type metricSample struct {
	Name   string            `json:"name"`
	Value  float64           `json:"value"`
	Labels map[string]string `json:"labels,omitempty"`
}

type metricsResponse struct {
	Server []metricSample `json:"server"`
	DB     []metricSample `json:"db"`
}

func fetchMetrics(metricsAddr string) (*metricsResponse, error) {
	if metricsAddr == "" {
		return &metricsResponse{}, nil
	}
	if strings.HasPrefix(metricsAddr, ":") {
		metricsAddr = "127.0.0.1" + metricsAddr
	}
	url := "http://" + metricsAddr + "/metrics"
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics endpoint returned %s", resp.Status)
	}
	return parsePrometheusMetrics(resp.Body)
}

func parsePrometheusMetrics(r io.Reader) (*metricsResponse, error) {
	out := &metricsResponse{}
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, "turnstone_") {
			continue
		}
		name, value, labels, err := parseMetricLine(line)
		if err != nil {
			continue
		}
		sample := metricSample{Name: name, Value: value, Labels: labels}
		if strings.HasPrefix(name, "turnstone_db_") {
			out.DB = append(out.DB, sample)
		} else {
			out.Server = append(out.Server, sample)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func parseMetricLine(line string) (string, float64, map[string]string, error) {
	labelStart := strings.Index(line, "{")
	var name string
	var rest string
	if labelStart >= 0 {
		labelEnd := strings.Index(line[labelStart:], "}")
		if labelEnd < 0 {
			return "", 0, nil, fmt.Errorf("unclosed label set")
		}
		labelEnd += labelStart
		name = line[:labelStart]
		rest = strings.TrimSpace(line[labelEnd+1:])
		labels := parseLabels(line[labelStart+1 : labelEnd])
		value, err := parseMetricValue(rest)
		if err != nil {
			return "", 0, nil, err
		}
		return name, value, labels, nil
	}
	parts := strings.Fields(line)
	if len(parts) != 2 {
		return "", 0, nil, fmt.Errorf("invalid metric line")
	}
	value, err := parseMetricValue(parts[1])
	if err != nil {
		return "", 0, nil, err
	}
	return parts[0], value, nil, nil
}

func parseLabels(raw string) map[string]string {
	labels := make(map[string]string)
	for _, part := range splitLabelPairs(raw) {
		eq := strings.Index(part, "=")
		if eq <= 0 {
			continue
		}
		key := part[:eq]
		val := strings.Trim(part[eq+1:], "\"")
		labels[key] = val
	}
	return labels
}

func splitLabelPairs(raw string) []string {
	var parts []string
	var current strings.Builder
	inQuotes := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '"' {
			inQuotes = !inQuotes
			current.WriteByte(ch)
			continue
		}
		if ch == ',' && !inQuotes {
			parts = append(parts, strings.TrimSpace(current.String()))
			current.Reset()
			continue
		}
		current.WriteByte(ch)
	}
	if current.Len() > 0 {
		parts = append(parts, strings.TrimSpace(current.String()))
	}
	return parts
}

func parseMetricValue(raw string) (float64, error) {
	if raw == "+Inf" {
		return math.Inf(1), nil
	}
	if raw == "-Inf" {
		return math.Inf(-1), nil
	}
	if raw == "NaN" {
		return 0, fmt.Errorf("NaN metric value")
	}
	return strconv.ParseFloat(raw, 64)
}
