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
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	feedSubscribe        = 1
	feedData             = 2
	moduleStateMachine   = 0x82
	moduleInstrument     = 0x83
	supervisorStateIndex = 3 // widgetStateMachine3 → "Supervisor State"

	instrumentEngineRunTime   = 305
	instrumentFuelDialDefault = 3 // widget dial "Fuel Level" (dial_textValue2 → ej. 77%)
)

type moduleTarget struct {
	ModuleID       string // ID único del enlace en el dashboard (attr name)
	GeneratorName  string
	GatewayUSBID   string
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
			strconv.Itoa(moduleInstrument):   moduleInstrumentIDs(t),
			strconv.Itoa(moduleStateMachine): []int{supervisorStateIndex},
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

type wsPending struct {
	needEngine     bool
	needFuel       bool
	needSupervisor bool
}

type wsSession struct {
	mu       sync.Mutex
	targets  []moduleTarget
	byModule map[string]moduleTarget
	results  map[string]ScrapeResult
	needed   map[string]*wsPending
	emitted  map[string]bool
	conn     *websocket.Conn
}

func (s *wsSession) addTarget(t moduleTarget) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.targets = append(s.targets, t)
	s.byModule[t.ModuleID] = t
	s.results[t.ModuleID] = ScrapeResult{Name: t.GeneratorName, FuelLevel: -1.0}

	key := t.GatewayUSBID + "|" + t.ModuleUSBID
	if _, ok := s.needed[key]; !ok {
		s.needed[key] = &wsPending{needEngine: true, needFuel: true, needSupervisor: true}
	}
}

func (s *wsSession) subscribe() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil || len(s.targets) == 0 {
		return nil
	}
	return s.conn.WriteJSON(buildFeedSubscription(s.targets))
}

func (s *wsSession) tryEmit(moduleID string, maps *mapHolder, out chan<- ScrapeResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.emitted[moduleID] {
		return
	}
	res, ok := s.results[moduleID]
	if !ok || !isCompleteResult(res) {
		return
	}
	t := s.byModule[moduleID]
	s.emitted[moduleID] = true

	maps.applyCoords(t.GatewayUSBID, &res)
	out <- res
}

func (s *wsSession) applyFeed(gateways map[string]struct {
	Modules map[string]map[string]json.RawMessage `json:"modules"`
}, maps *mapHolder, out chan<- ScrapeResult) {
	instrumentKey := strconv.Itoa(moduleInstrument)
	stateMachineKey := strconv.Itoa(moduleStateMachine)
	supervisorKey := strconv.Itoa(supervisorStateIndex)

	for gwID, gw := range gateways {
		for modID, mod := range gw.Modules {
			key := gwID + "|" + modID

			s.mu.Lock()
			var moduleIDs []string
			for id, t := range s.byModule {
				if t.GatewayUSBID == gwID && t.ModuleUSBID == modID {
					moduleIDs = append(moduleIDs, id)
				}
			}
			s.mu.Unlock()

			if len(moduleIDs) == 0 {
				continue
			}

			for _, moduleID := range moduleIDs {
				s.mu.Lock()
				t := s.byModule[moduleID]
				res := s.results[moduleID]

				if rawInstruments, ok := mod[instrumentKey]; ok {
					var instruments map[string]instrumentFeed
					if err := json.Unmarshal(rawInstruments, &instruments); err == nil {
						for idStr, feed := range instruments {
							id, _ := strconv.Atoi(idStr)
							switch id {
							case instrumentEngineRunTime:
								res.EngineRunTime = instrumentValue(feed)
							case t.FuelDialID:
								res.FuelLevel = math.Round(feed.Value*100) / 100
							}
						}
					}
				}

				if rawStates, ok := mod[stateMachineKey]; ok {
					var states map[string]string
					if err := json.Unmarshal(rawStates, &states); err == nil {
						if state, ok := states[supervisorKey]; ok && state != "" && state != "#N/A#" {
							res.SupervisorState = state
						}
					}
				}

				s.results[moduleID] = res

				if p, ok := s.needed[key]; ok {
					if res.EngineRunTime != "" {
						p.needEngine = false
					}
					if res.FuelLevel != -1.0 {
						p.needFuel = false
					}
					if res.SupervisorState != "" {
						p.needSupervisor = false
					}
					if !p.needEngine && !p.needFuel && !p.needSupervisor {
						delete(s.needed, key)
					}
				}
				s.mu.Unlock()

				s.tryEmit(moduleID, maps, out)
			}
		}
	}
}

func (s *wsSession) pendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.needed)
}

// consumeInstrumentsWebSocket recibe targets por canal, abre WS al primer módulo
// y publica cada generador completo en out en cuanto llegan los datos.
func consumeInstrumentsWebSocket(targetsIn <-chan moduleTarget, out chan<- ScrapeResult, maps *mapHolder, timeout time.Duration) {
	defer close(out)

	wsURL := os.Getenv("DSE_REALTIME_URL")
	if wsURL == "" {
		wsURL = "wss://www.dsewebnet.com/user"
	}

	u, err := url.Parse("https://www.dsewebnet.com")
	if err != nil {
		log.Printf("WebSocket: %v\n", err)
		return
	}

	session := &wsSession{
		byModule: make(map[string]moduleTarget),
		results:  make(map[string]ScrapeResult),
		needed:   make(map[string]*wsPending),
		emitted:  make(map[string]bool),
	}

	var (
		connMu       sync.Mutex
		resubMu      sync.Mutex
		resubTimer   *time.Timer
		targetsDone  = make(chan struct{})
		readDone     = make(chan struct{})
		deadline     = time.Now().Add(timeout)
	)

	scheduleSubscribe := func() {
		resubMu.Lock()
		defer resubMu.Unlock()
		if resubTimer != nil {
			resubTimer.Stop()
		}
		resubTimer = time.AfterFunc(75*time.Millisecond, func() {
			connMu.Lock()
			defer connMu.Unlock()
			if session.conn != nil {
				if err := session.subscribe(); err != nil {
					log.Printf("WebSocket: error re-suscribiendo: %v\n", err)
				}
			}
		})
	}

	dialWebSocket := func() error {
		connMu.Lock()
		defer connMu.Unlock()
		if session.conn != nil {
			return nil
		}
		hdr := http.Header{}
		for _, c := range client.Jar.Cookies(u) {
			hdr.Add("Cookie", c.Name+"="+c.Value)
		}
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, hdr)
		if err != nil {
			return fmt.Errorf("error conectando WebSocket: %w", err)
		}
		session.conn = conn
		return session.subscribe()
	}

	go func() {
		defer close(targetsDone)
		for t := range targetsIn {
			session.addTarget(t)

			connMu.Lock()
			needsDial := session.conn == nil
			connMu.Unlock()

			if needsDial {
				if err := dialWebSocket(); err != nil {
					log.Printf("WebSocket: %v\n", err)
					return
				}
			} else {
				scheduleSubscribe()
			}
		}

		resubMu.Lock()
		if resubTimer != nil {
			resubTimer.Stop()
		}
		resubMu.Unlock()

		connMu.Lock()
		if session.conn != nil {
			_ = session.subscribe()
		}
		connMu.Unlock()
	}()

	go func() {
		defer close(readDone)
		for {
			if time.Now().After(deadline) {
				return
			}

			connMu.Lock()
			conn := session.conn
			connMu.Unlock()
			if conn == nil {
				select {
				case <-targetsDone:
					return
				case <-time.After(50 * time.Millisecond):
					continue
				}
			}

			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			_, data, err := conn.ReadMessage()
			if err != nil {
				connMu.Lock()
				if session.conn == conn {
					_ = session.conn.Close()
					session.conn = nil
				}
				connMu.Unlock()

				if time.Now().After(deadline) {
					return
				}
				select {
				case <-targetsDone:
					return
				default:
					time.Sleep(50 * time.Millisecond)
					continue
				}
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
				Modules map[string]map[string]json.RawMessage `json:"modules"`
			}
			if err := json.Unmarshal(rawFeed, &gateways); err != nil {
				continue
			}

			session.applyFeed(gateways, maps, out)

			if session.pendingCount() == 0 {
				select {
				case <-targetsDone:
					return
				default:
				}
			}
		}
	}()

	<-targetsDone
	<-readDone

	connMu.Lock()
	if session.conn != nil {
		_ = session.conn.Close()
	}
	connMu.Unlock()

	if n := session.pendingCount(); n > 0 {
		log.Printf("WebSocket: timeout, faltan datos de %d módulo(s)\n", n)
	}
}
