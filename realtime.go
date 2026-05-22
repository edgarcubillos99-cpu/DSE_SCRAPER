package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	feedSubscribe    = 1
	feedData         = 2
	moduleInstrument = 0x83

	instrumentEngineRunTime   = 305
	instrumentFuelDialDefault = 3 // widget dial "Fuel Level" (dial_textValue2 → ej. 77%)
)

type moduleTarget struct {
	ModuleID      string // ID único del enlace en el dashboard (attr name)
	GeneratorName string
	GatewayUSBID  string
	ModuleUSBID   string
	FuelDialID    int
}

type instrumentFeed struct {
	Value    float64 `json:"value"`
	RawValue float64 `json:"rawValue"`
	Units    string  `json:"units"`
}

func parseModuleTargets(html string, gen Generator) (moduleTarget, error) {
	gwRe := regexp.MustCompile(`setGatewayUSBID\('([^']+)'\)`)
	modRe := regexp.MustCompile(`setModuleUSBID\('([^']+)'\)`)

	gw := gwRe.FindStringSubmatch(html)
	mod := modRe.FindStringSubmatch(html)
	if len(gw) < 2 || len(mod) < 2 {
		return moduleTarget{}, fmt.Errorf("no se encontraron IDs de gateway/módulo")
	}

	if !hasEngineTab(html) {
		return moduleTarget{}, fmt.Errorf("módulo sin pestaña Engine")
	}

	return moduleTarget{
		ModuleID:      gen.ID,
		GeneratorName: gen.Name,
		GatewayUSBID:  gw[1],
		ModuleUSBID:   mod[1],
		FuelDialID:    parseFuelDialID(html),
	}, nil
}

func hasEngineTab(html string) bool {
	return strings.Contains(html, "tab=3") &&
		(strings.Contains(html, ">Engine</a>") || strings.Contains(html, "Engine Run Time"))
}

func parseFuelDialID(html string) int {
	dialID := instrumentFuelDialDefault
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`"label"\s*:\s*"Fuel Level"[^}]*"instrument_id_dial"\s*:\s*(\d+)`),
		regexp.MustCompile(`"instrument_id_dial"\s*:\s*(\d+)[^}]*"label"\s*:\s*"Fuel Level"`),
	}
	for _, re := range patterns {
		if m := re.FindStringSubmatch(html); len(m) > 1 {
			if id, err := strconv.Atoi(m[1]); err == nil {
				return id
			}
		}
	}
	return dialID
}

func moduleInstrumentIDs(t moduleTarget) []int {
	return []int{instrumentEngineRunTime, t.FuelDialID}
}

func buildFeedSubscription(targets []moduleTarget) map[string]any {
	gateways := make(map[string]any)

	for _, t := range targets {
		gwEntry, ok := gateways[t.GatewayUSBID].(map[string]any)
		if !ok {
			gwEntry = map[string]any{"modules": map[string]any{}}
			gateways[t.GatewayUSBID] = gwEntry
		}
		modules := gwEntry["modules"].(map[string]any)
		modules[t.ModuleUSBID] = map[string]any{
			strconv.Itoa(moduleInstrument): moduleInstrumentIDs(t),
		}
	}

	return map[string]any{
		strconv.Itoa(feedSubscribe): gateways,
	}
}

func instrumentValue(feed instrumentFeed) string {
	if feed.Units == "Hours" {
		return fmt.Sprintf("%.2f Hours", feed.Value)
	}
	return fmt.Sprintf("%.2f", feed.Value)
}

func isCompleteResult(res ScrapeResult) bool {
	// Comparamos con -1.0 en lugar de ""
	return res.EngineRunTime != "" && res.FuelLevel != -1.0
}

func fetchInstrumentsViaWebSocket(targets []moduleTarget, timeout time.Duration) (map[string]ScrapeResult, error) {
	wsURL := os.Getenv("DSE_REALTIME_URL")
	if wsURL == "" {
		wsURL = "wss://www.dsewebnet.com/user"
	}

	u, err := url.Parse("https://www.dsewebnet.com")
	if err != nil {
		return nil, err
	}

	hdr := http.Header{}
	for _, c := range client.Jar.Cookies(u) {
		hdr.Add("Cookie", c.Name+"="+c.Value)
	}

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, hdr)
	if err != nil {
		return nil, fmt.Errorf("error conectando WebSocket: %w", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(buildFeedSubscription(targets)); err != nil {
		return nil, fmt.Errorf("error enviando suscripción: %w", err)
	}

	type pending struct {
		needEngine bool
		needFuel   bool
	}
	needed := make(map[string]*pending)
	results := make(map[string]ScrapeResult)
	for _, t := range targets {
		key := t.GatewayUSBID + "|" + t.ModuleUSBID
		if _, ok := needed[key]; !ok {
			needed[key] = &pending{needEngine: true, needFuel: true}
		}
		// Inicializamos FuelLevel en -1.0
		results[t.ModuleID] = ScrapeResult{Name: t.GeneratorName, FuelLevel: -1.0}
	}

	deadline := time.Now().Add(timeout)
	_ = conn.SetReadDeadline(deadline)

	for time.Now().Before(deadline) {
		_, data, err := conn.ReadMessage()
		if err != nil {
			break
		}

		var msg map[string]json.RawMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		rawFeed, ok := msg[strconv.Itoa(feedData)]
		if !ok {
			continue
		}

		var gateways map[string]struct {
			Modules map[string]map[string]map[string]instrumentFeed `json:"modules"`
		}
		if err := json.Unmarshal(rawFeed, &gateways); err != nil {
			continue
		}

		feedKey := strconv.Itoa(moduleInstrument)
		for gwID, gw := range gateways {
			for modID, mod := range gw.Modules {
				key := gwID + "|" + modID
				instruments, ok := mod[feedKey]
				if !ok {
					continue
				}

				var matched bool
				for _, t := range targets {
					if t.GatewayUSBID != gwID || t.ModuleUSBID != modID {
						continue
					}
					matched = true
					res := results[t.ModuleID]
					for idStr, feed := range instruments {
						id, _ := strconv.Atoi(idStr)
						switch id {
						case instrumentEngineRunTime:
							res.EngineRunTime = instrumentValue(feed)
						case t.FuelDialID:
							// Asignamos el valor como número flotante redondeado a 2 decimales
							res.FuelLevel = math.Round(feed.Value*100) / 100
						}
					}
					results[t.ModuleID] = res
				}
				if !matched {
					continue
				}

				if p, ok := needed[key]; ok {
					for _, t := range targets {
						if t.GatewayUSBID != gwID || t.ModuleUSBID != modID {
							continue
						}
						res := results[t.ModuleID]
						if res.EngineRunTime != "" {
							p.needEngine = false
						}
						// Cambiamos la validación a -1.0
						if res.FuelLevel != -1.0 {
							p.needFuel = false
						}
					}
					if !p.needEngine && !p.needFuel {
						delete(needed, key)
					}
				}
			}
		}

		if len(needed) == 0 {
			break
		}
	}

	if len(needed) > 0 {
		log.Printf("WebSocket: timeout, faltan datos de %d módulo(s)\n", len(needed))
	}

	return results, nil
}
