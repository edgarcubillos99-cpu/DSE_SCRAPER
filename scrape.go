package main

import (
	"fmt"
	"io"
	"log"
	"sync"
	"time"
)

type mapHolder struct {
	mu   sync.RWMutex
	data map[string]mapDevice
}

func (h *mapHolder) set(devices map[string]mapDevice) {
	h.mu.Lock()
	h.data = devices
	h.mu.Unlock()
}

func (h *mapHolder) applyCoords(gatewayUSBID string, res *ScrapeResult) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.data == nil {
		return
	}
	if dev, ok := h.data[gatewayUSBID]; ok {
		lat, lng := dev.Latitude, dev.Longitude
		res.Latitude = &lat
		res.Longitude = &lng
	}
}

func fetchModuleTarget(baseURL string, gen Generator) (moduleTarget, error) {
	engineURL := fmt.Sprintf("%s/module.php?id=%s&tab=3", baseURL, gen.ID)
	resp, err := client.Get(engineURL)
	if err != nil {
		return moduleTarget{}, fmt.Errorf("error de conexión")
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return moduleTarget{}, fmt.Errorf("error leyendo página Engine")
	}
	return parseModuleTargets(string(body), gen)
}

// produceModuleTargets resuelve módulos con workers y publica cada target en el canal.
func produceModuleTargets(baseURL string, generators []Generator, numWorkers int, out chan<- moduleTarget) {
	defer close(out)

	jobs := make(chan Generator, len(generators))
	results := make(chan moduleTarget, len(generators))

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for gen := range jobs {
				t, err := fetchModuleTarget(baseURL, gen)
				if err != nil {
					if err.Error() != "módulo sin pestaña Engine" {
						log.Printf("Módulo %s: %s\n", gen.ID, err)
					}
					continue
				}
				results <- t
			}
		}()
	}

	for _, gen := range generators {
		jobs <- gen
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	for t := range results {
		out <- t
	}
}

// runScrapePipeline ejecuta mapa, HTTP y WebSocket en paralelo; los resultados salen por canal.
func runScrapePipeline(baseURL string, generators []Generator, numWorkers int, wsTimeout time.Duration) []ScrapeResult {
	targetCh := make(chan moduleTarget, len(generators))
	resultCh := make(chan ScrapeResult, len(generators))

	maps := &mapHolder{}
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		devices, err := fetchMapDevices(baseURL)
		if err != nil {
			log.Printf("Advertencia: coordenadas del mapa no disponibles: %v\n", err)
			devices = map[string]mapDevice{}
		}
		maps.set(devices)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		produceModuleTargets(baseURL, generators, numWorkers, targetCh)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		consumeInstrumentsWebSocket(targetCh, resultCh, maps, wsTimeout)
	}()

	wg.Wait()

	var finalData []ScrapeResult
	for res := range resultCh {
		finalData = append(finalData, res)
	}
	return finalData
}
