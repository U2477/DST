package main

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ANSI color codes
const (
	Gray    = "\033[90m"
	Green   = "\033[32m"
	Blue    = "\033[34m"
	Magenta = "\033[36m"
	Reset   = "\033[0m"
)

// Configuration
const (
	bufferSize       = 100
	connectionTimeout = 3 * time.Second
	retryAttempts     = 0
)

// ASCII Art Banner
const banner = `
██████╗ ███████╗██╗   ██╗██╗   ██╗ █████╗ ███╗   ██╗
██╔══██╗██╔════╝██║   ██║██║   ██║██╔══██╗████╗  ██║
██║  ██║█████╗  ██║   ██║██║   ██║███████║██╔██╗ ██║
██║  ██║██╔══╝  ╚██╗ ██╔╝██║   ██║██╔══██║██║╚██╗██║
██████╔╝███████╗ ╚████╔╝ ╚██████╔╝██║  ██║██║ ╚████║
╚═════╝ ╚══════╝  ╚═══╝   ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═══╝
`

// Result represents the outcome of checking a domain
type Result struct {
	Domain     string
	StatusCode int
	Error      error
	Duration   time.Duration
}

// StatusChecker manages the domain checking process
type StatusChecker struct {
	client            *http.Client
	successfulDomains []string
	mu                sync.Mutex
	startTime         time.Time
	totalDomains      int
	processedDomains  int
}

// NewStatusChecker initializes the checker with a high-performance HTTP client
func NewStatusChecker(totalDomains int) *StatusChecker {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   connectionTimeout,
			KeepAlive: 10 * time.Second,
		}).DialContext,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		MaxIdleConns:          500,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       10 * time.Second,
		DisableKeepAlives:     false,
		DisableCompression:    true,
	}

	return &StatusChecker{
		client: &http.Client{
			Transport: transport,
			Timeout:   connectionTimeout,
		},
		startTime:    time.Now(),
		totalDomains: totalDomains,
	}
}

// checkDomain performs a fast HTTP GET request
func (sc *StatusChecker) checkDomain(domain string) Result {
	start := time.Now()
	if !strings.HasPrefix(domain, "http") {
		domain = "https://" + domain
	}

	resp, err := sc.client.Get(domain)
	if err != nil {
		return Result{
			Domain:   domain,
			Error:    err,
			Duration: time.Since(start),
		}
	}
	defer resp.Body.Close()

	// Record successful domains
	if resp.StatusCode >= 1 && resp.StatusCode <= 500 {
		sc.mu.Lock()
		sc.successfulDomains = append(sc.successfulDomains, domain)
		sc.mu.Unlock()
	}

	return Result{
		Domain:     domain,
		StatusCode: resp.StatusCode,
		Duration:   time.Since(start),
	}
}

// worker processes domains from the channel
func (sc *StatusChecker) worker(domains <-chan string, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	for domain := range domains {
		results <- sc.checkDomain(domain)
	}
}

// processResults formats and prints results in real-time
func (sc *StatusChecker) processResults(results <-chan Result) {
	for result := range results {
		sc.mu.Lock()
		sc.processedDomains++
		percentage := float64(sc.processedDomains) / float64(sc.totalDomains) * 100
		sc.mu.Unlock()

		if result.Error != nil {
			fmt.Printf("%s%-50s [000] Failed (%.2fs) ---> %6.1f%%%s\n",
				Gray, result.Domain, result.Duration.Seconds(), percentage, Reset)
		} else {
			fmt.Printf("%s%-50s [%d] %s (%.2fs) ---> %6.1f%%%s\n",
				Green, result.Domain, result.StatusCode, http.StatusText(result.StatusCode),
				result.Duration.Seconds(), percentage, Reset)
		}
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("%sUsage: %s <hostfile>%s\n", Green, os.Args[0], Reset)
		os.Exit(1)
	}

	// Open file and read domains
	file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Printf("%sError: Unable to open file - %v%s\n", Magenta, err, Reset)
		os.Exit(1)
	}
	defer file.Close()

	var domainsList []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		domain := strings.TrimSpace(scanner.Text())
		if domain != "" {
			domainsList = append(domainsList, domain)
		}
	}

	totalDomains := len(domainsList)
	if totalDomains == 0 {
		fmt.Printf("%sNo domains found in the file.%s\n", Magenta, Reset)
		os.Exit(1)
	}

	// Display the banner
	fmt.Printf("\n%s%s%s\n", Magenta, banner, Reset)

	// Ask user for desired speed
	fmt.Print("Enter desired speed (number of workers): ")
	var numWorkers int
	for {
		input := ""
		fmt.Scanln(&input)
		speed, err := strconv.Atoi(input)
		if err != nil || speed <= 0 {
			fmt.Print("Invalid input. Enter a positive number: ")
			continue
		}
		numWorkers = speed
		break
	}

	// Initialize checker
	checker := NewStatusChecker(totalDomains)
	domains := make(chan string, bufferSize)
	results := make(chan Result, bufferSize)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go checker.worker(domains, results, &wg)
	}

	// Feed domains into the channel
	go func() {
		for _, domain := range domainsList {
			domains <- domain
		}
		close(domains)
	}()

	// Start result processor
	go func() {
		wg.Wait()
		close(results)
	}()

	// Display results
	checker.processResults(results)

	// Summary
	duration := time.Since(checker.startTime)
	fmt.Printf("\n%s=== Summary ===%s\n", Magenta, Reset)
	fmt.Printf("Total domains checked: %d\n", totalDomains)
	fmt.Printf("Successful domains: %d\n", len(checker.successfulDomains))
	fmt.Printf("Failed domains: %d\n", totalDomains-len(checker.successfulDomains))
	fmt.Printf("Total time taken: %.2fs\n", duration.Seconds())
	if totalDomains > 0 {
		fmt.Printf("Average time per domain: %.2fs\n", duration.Seconds()/float64(totalDomains))
	}
}
