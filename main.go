package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/joho/godotenv"
)

type Generator struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type ScrapeResult struct {
	Name          string `json:"name"`
	EngineRunTime string `json:"engine_run_time"`
	FuelLevel     string `json:"fuel_level"`
	Error         string `json:"error,omitempty"`
}

// Cliente global que mantendrá la sesión (las cookies)
var client *http.Client

func main() {
	// Cargar variables de entorno
	err := godotenv.Load()
	if err != nil {
		log.Println("Advertencia: No se encontró el archivo .env, usando variables del sistema si existen")
	}

	// Inicializar el gestor de cookies
	jar, err := cookiejar.New(nil)
	if err != nil {
		log.Fatal("Error creando cookie jar:", err)
	}

	// El cliente HTTP ahora guardará y enviará cookies automáticamente
	client = &http.Client{
		Jar: jar,
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	http.HandleFunc("/api/generators", handleScrape)

	fmt.Printf("Servidor iniciado en el puerto %s...\n", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleScrape(w http.ResponseWriter, r *http.Request) {
	log.Println("1. Recibiendo petición, iniciando autenticación...")

	err := authenticate()
	if err != nil {
		log.Printf("Error de autenticación: %v\n", err)
		http.Error(w, "Error de autenticación en DSE", http.StatusInternalServerError)
		return
	}

	log.Println("2. Autenticación exitosa. Obteniendo lista de generadores...")

	baseURL := os.Getenv("DSE_BASE_URL")
	generators, err := fetchGeneratorsList(baseURL)
	if err != nil {
		log.Printf("Error al obtener la lista: %v\n", err)
		http.Error(w, "Error al obtener la lista", http.StatusInternalServerError)
		return
	}

	log.Println("3. Obteniendo IDs de módulos y datos en tiempo real...")

	workersStr := os.Getenv("WORKERS")
	numWorkers, err := strconv.Atoi(workersStr)
	if err != nil || numWorkers <= 0 {
		numWorkers = 5
	}

	targets, _ := fetchModuleTargets(baseURL, generators, numWorkers)
	if len(targets) == 0 {
		log.Println("Error: no se pudo resolver ningún módulo con pestaña Engine")
		http.Error(w, "Error al resolver módulos DSE", http.StatusInternalServerError)
		return
	}

	wsTimeout := 45 * time.Second
	if s := os.Getenv("REALTIME_TIMEOUT_SEC"); s != "" {
		if sec, err := strconv.Atoi(s); err == nil && sec > 0 {
			wsTimeout = time.Duration(sec) * time.Second
		}
	}

	instrumentData, err := fetchInstrumentsViaWebSocket(targets, wsTimeout)
	if err != nil {
		log.Printf("Error WebSocket: %v\n", err)
		http.Error(w, "Error obteniendo datos en tiempo real", http.StatusInternalServerError)
		return
	}

	var finalData []ScrapeResult
	for _, gen := range generators {
		res, ok := instrumentData[gen.ID]
		if !ok {
			continue
		}
		if !isCompleteResult(res) {
			continue
		}
		res.Name = gen.Name
		finalData = append(finalData, res)
	}

	// Justo antes de devolver el JSON:
	log.Println("4. Scraping finalizado. Devolviendo JSON al cliente.")

	// 5. Devolver JSON indentado para lectura humana
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(finalData)
}

// authenticate obtiene los tokens CSRF del formulario y envía el login.
func authenticate() error {
	loginURL := os.Getenv("DSE_LOGIN_URL")
	user := os.Getenv("DSE_USER")
	password := os.Getenv("DSE_PASSWORD")

	if user == "" || password == "" {
		return fmt.Errorf("DSE_USER o DSE_PASSWORD no están configurados en .env")
	}

	// 1. GET de la página de login para obtener cookies de sesión y tokens CSRF
	resp, err := client.Get(loginURL)
	if err != nil {
		return fmt.Errorf("error al cargar la página de login: %v", err)
	}
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("error parseando la página de login: %v", err)
	}

	csrfID, _ := doc.Find(`input[name="login[_csrfID]"]`).Attr("value")
	csrfKey, _ := doc.Find(`input[name="login[_csrfKey]"]`).Attr("value")
	if csrfID == "" || csrfKey == "" {
		return fmt.Errorf("no se encontraron tokens CSRF en la página de login")
	}

	// 2. POST con los mismos campos que envía el formulario del navegador
	formData := url.Values{
		"login[_csrfID]":    {csrfID},
		"login[_csrfKey]":   {csrfKey},
		"login[username]":   {user},
		"login[password]":   {password},
		"login[btnLogin]":   {"Login"},
	}

	resp, err = client.PostForm(loginURL, formData)
	if err != nil {
		return fmt.Errorf("error al hacer POST al login: %v", err)
	}
	defer resp.Body.Close()

	finalURL := resp.Request.URL.String()
	if strings.Contains(finalURL, "login.php") {
		return fmt.Errorf("el login falló, seguimos en la página de login (revisa credenciales)")
	}

	fmt.Println("Autenticación exitosa.")
	return nil
}

func fetchGeneratorsList(baseURL string) ([]Generator, error) {
	// Ya no necesitamos inyectar cookies manualmente, el `client` global lo hace
	resp, err := client.Get(baseURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, err
	}

	var generators []Generator
	doc.Find("div#group_1 a").Each(func(i int, s *goquery.Selection) {
		id, _ := s.Attr("name")
		gwName, _ := s.Attr("data-gwname")

		if id != "" && gwName != "" {
			generators = append(generators, Generator{
				ID:   id,
				Name: gwName,
			})
		}
	})

	return generators, nil
}

func fetchModuleTargets(baseURL string, generators []Generator, numWorkers int) ([]moduleTarget, map[string]string) {
	jobs := make(chan Generator, len(generators))
	type targetResult struct {
		target   moduleTarget
		err      string
		moduleID string
	}
	results := make(chan targetResult, len(generators))
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for gen := range jobs {
				targetURL := fmt.Sprintf("%s/module.php?id=%s&tab=3", baseURL, gen.ID)
				resp, err := client.Get(targetURL)
				if err != nil {
					results <- targetResult{moduleID: gen.ID, err: "Error de conexión"}
					continue
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					results <- targetResult{moduleID: gen.ID, err: "Error leyendo página"}
					continue
				}
				t, err := parseModuleTargets(string(body), gen)
				if err != nil {
					results <- targetResult{moduleID: gen.ID, err: err.Error()}
					continue
				}
				results <- targetResult{target: t}
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

	var targets []moduleTarget
	errors := make(map[string]string)
	for res := range results {
		if res.err != "" {
			// Sin pestaña Engine u otro motivo: omitir silenciosamente del scrape
			if res.err != "módulo sin pestaña Engine" {
				errors[res.moduleID] = res.err
			}
			continue
		}
		targets = append(targets, res.target)
	}
	return targets, errors
}
