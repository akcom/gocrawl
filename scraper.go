package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

type ParseFunc func(*Scraper, string, string)

type ScrapeTuple struct {
	url       string
	parseFunc ParseFunc
}

type Scraper struct {
	channelCount int
	scrapeChan   chan ScrapeTuple
	addChan      chan ScrapeTuple
	finishChan   chan int
	wg           sync.WaitGroup
}

func NewScraper() *Scraper {
	return &Scraper{
		channelCount: 32,
		scrapeChan:   make(chan ScrapeTuple, 1000),
		addChan:      make(chan ScrapeTuple, 100),
		finishChan:   make(chan int, 100),
	}
}

func (s *Scraper) scrapeURL(url string, f ParseFunc) {
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error opening '%s'", url)
		return
	}
	defer resp.Body.Close()
	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading body of '%s'", url)
		return
	}
	f(s, url, string(bytes))
}

func (s *Scraper) goRun() {
	for {
		tuple, ok := <-s.scrapeChan
		if !ok {
			break
		}
		s.scrapeURL(tuple.url, tuple.parseFunc)
		s.wg.Done()
	}
}

//run returns immediately
//the result is a channel that can be waited on
//to signal when all URL's have been scraped
func (s *Scraper) Run() <-chan int {
	done := make(chan int)

	go s.processAdds()
	//spool up the scraper goroutines
	for i := 0; i < s.channelCount; i++ {
		go s.goRun()
	}

	go func() {
		s.wg.Wait()
		close(s.addChan)
		close(s.scrapeChan)
		done <- 1
	}()
	return done
}

func (s *Scraper) processAdds() {
	buf := make([]ScrapeTuple, 0)
	for {
		select {
		case v, ok := <-s.addChan:
			if !ok {
				break
			}
			buf = append(buf, v)
		default:
		}

		if len(buf) == 0 {
			continue
		}

		i := 0
		done := false
		for i < len(buf) {
			select {
			case s.scrapeChan <- buf[i]:
				i++
			default:
				done = true
			}
			if done {
				break
			}
		}
		if i == len(buf) {
			buf = make([]ScrapeTuple, 0)
		} else {
			tmp := make([]ScrapeTuple, len(buf)-i)
			copy(tmp, buf[i:])
			buf = tmp
		}
	}
}

func (s *Scraper) AddURL(url string, parseFunc ParseFunc) {
	s.wg.Add(1)
	s.addChan <- ScrapeTuple{url, parseFunc}
}
