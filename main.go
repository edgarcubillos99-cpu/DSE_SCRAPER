package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/joho/godotenv"
)

type Generator struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type ScrapeResult struct {
	Name           string   `json:"name"`
	EngineRunTime  string   `json:"engine_run_time"`
	FuelLevel      float64  `json:"fuel_level"`
	SupervisorState string   `json:"supervisor_state,omitempty"`
	Latitude       *float64 `json:"latitude,omitempty"`
	Longitude      *float64 `json:"longitude,omitempty"`
	Error          string   `json:"error,omitempty"`
}

// Cliente global que mantendrá la sesión (las cookies)
var client *http.Client

func main() {
	// Cargar .env local (en Docker Compose las variables llegan por env_file, sin archivo .env)
	if err := godotenv.Load(); err != nil && os.Getenv("DSE_USER") == "" && os.Getenv("DSE_PASSWORD") == "" {
		log.Println("Advertencia: no hay .env ni variables DSE_USER/DSE_PASSWORD en el entorno")
	}

	// Inicializar el gestor de cookies
	jar, err := cookiejar.New(nil)
	if err != nil {
		log.Fatal("Error creando cookie jar:", err)
	}

	client = &http.Client{
		Jar:     jar,
		Timeout: 30 * time.Second,
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

	wsTimeout := 45 * time.Second
	if s := os.Getenv("REALTIME_TIMEOUT_SEC"); s != "" {
		if sec, err := strconv.Atoi(s); err == nil && sec > 0 {
			wsTimeout = time.Duration(sec) * time.Second
		}
	}

	finalData := runScrapePipeline(baseURL, generators, numWorkers, wsTimeout)
	if len(finalData) == 0 {
		log.Println("Error: no se obtuvieron datos completos de ningún generador")
		http.Error(w, "Error al resolver módulos DSE", http.StatusInternalServerError)
		return
	}

	// Justo antes de devolver el JSON:
	log.Println("4. Scraping finalizado. Devolviendo JSON al cliente.")

	// 5. Devolver JSON indentado para lectura humana
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(finalData)
}

func dseSiteURL() (*url.URL, error) {
	base := os.Getenv("DSE_BASE_URL")
	if base == "" {
		base = os.Getenv("DSE_LOGIN_URL")
	}
	return url.Parse(base)
}

func clearSessionCookies() {
	u, err := dseSiteURL()
	if err != nil {
		return
	}
	client.Jar.SetCookies(u, nil)
}

// isAuthenticated comprueba si las cookies actuales siguen siendo válidas.
func isAuthenticated() bool {
	baseURL := os.Getenv("DSE_BASE_URL")
	if baseURL == "" {
		return false
	}
	resp, err := client.Get(baseURL)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.Request == nil || resp.Request.URL == nil {
		return false
	}
	return !strings.Contains(resp.Request.URL.String(), "login.php")
}

// authenticate reutiliza la sesión activa o obtiene tokens CSRF y envía el login.
func authenticate() error {
	if isAuthenticated() {
		return nil
	}

	loginURL := os.Getenv("DSE_LOGIN_URL")
	user := os.Getenv("DSE_USER")
	password := os.Getenv("DSE_PASSWORD")

	if user == "" || password == "" {
		return fmt.Errorf("DSE_USER o DSE_PASSWORD no están configurados en .env")
	}

	// Sin sesión válida: limpiar cookies para que login.php devuelva el formulario con CSRF
	clearSessionCookies()

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
		// Reintento tras limpiar cookies (p. ej. sesión a medias)
		clearSessionCookies()
		resp, err = client.Get(loginURL)
		if err != nil {
			return fmt.Errorf("error al recargar la página de login: %v", err)
		}
		doc, err = goquery.NewDocumentFromReader(resp.Body)
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("error parseando la página de login: %v", err)
		}
		csrfID, _ = doc.Find(`input[name="login[_csrfID]"]`).Attr("value")
		csrfKey, _ = doc.Find(`input[name="login[_csrfKey]"]`).Attr("value")
		if csrfID == "" || csrfKey == "" {
			return fmt.Errorf("no se encontraron tokens CSRF en la página de login")
		}
	}

	// 2. POST con los mismos campos que envía el formulario del navegador
	formData := url.Values{
		"login[_csrfID]":  {csrfID},
		"login[_csrfKey]": {csrfKey},
		"login[username]": {user},
		"login[password]": {password},
		"login[btnLogin]": {"Login"},
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

