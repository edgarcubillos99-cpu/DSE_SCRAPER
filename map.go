package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

type mapDevice struct {
	USBID       string  `json:"usbID"`
	DisplayName string  `json:"displayName"`
	Latitude    float64 `json:"latitude"`
	Longitude   float64 `json:"longitude"`
}

func fetchMapDevices(baseURL string) (map[string]mapDevice, error) {
	mapURL := strings.TrimRight(baseURL, "/") + "/?tab=1"
	resp, err := client.Get(mapURL)
	if err != nil {
		return nil, fmt.Errorf("error cargando página Map: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return parseMapDevicesJSON(string(body))
}

func parseMapDevicesJSON(html string) (map[string]mapDevice, error) {
	const marker = "var devices = "
	idx := strings.Index(html, marker)
	if idx < 0 {
		return nil, fmt.Errorf("objeto devices no encontrado en la página Map")
	}

	start := idx + len(marker)
	brace := strings.Index(html[start:], "{")
	if brace < 0 {
		return nil, fmt.Errorf("JSON de devices inválido")
	}
	start += brace

	depth := 0
	end := -1
parseLoop:
	for i := start; i < len(html); i++ {
		switch html[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				end = i + 1
				break parseLoop
			}
		}
	}
	if end < 0 {
		return nil, fmt.Errorf("JSON de devices incompleto")
	}

	var raw map[string]mapDevice
	if err := json.Unmarshal([]byte(html[start:end]), &raw); err != nil {
		return nil, fmt.Errorf("error parseando devices: %w", err)
	}
	return raw, nil
}
