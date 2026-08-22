// Command roomrace reproduces and measures the two new-room -> first-message
// races against a real NATS server, using the production subject builders from
// pkg/subject so the topology under test matches what the services publish.
//
//	problem 1 (DM)      : new_message rides chat.user.{B}.event.room, which B
//	                      holds from login. Loss is decided by the CLIENT.
//	problem 2 (channel) : new_message rides chat.room.{id}.event, which B only
//	                      subscribes to after handling subscription.update.
//	                      Loss is decided by the TRANSPORT.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

type options struct {
	subURL     string
	pubURL     string
	label      string
	iterations int
	renderMS   int
	sendMS     string
	settleMS   int
	jsonOut    bool
}

func main() {
	var opt options
	flag.StringVar(&opt.subURL, "sub-url", nats.DefaultURL, "NATS URL the simulated client B connects to")
	flag.StringVar(&opt.pubURL, "pub-url", "", "NATS URL the simulated services publish from (default: same as -sub-url)")
	flag.StringVar(&opt.label, "label", "single-server", "label for this topology in the report")
	flag.IntVar(&opt.iterations, "iterations", 200, "iterations per data point")
	flag.IntVar(&opt.renderMS, "render-ms", 30, "client B: ms spent rendering the new room before it subscribes / accepts messages")
	flag.StringVar(&opt.sendMS, "send-ms", "0,1,5,10,25,50,100", "comma-separated delays between subscription.update and the first message")
	flag.IntVar(&opt.settleMS, "settle-ms", 400, "ms to wait for late delivery before declaring a message lost")
	flag.BoolVar(&opt.jsonOut, "json", false, "emit JSON instead of tables")
	flag.Parse()

	if opt.pubURL == "" {
		opt.pubURL = opt.subURL
	}

	rep, err := run(&opt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "roomrace: %v\n", err)
		os.Exit(1)
	}

	if opt.jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			fmt.Fprintf(os.Stderr, "roomrace: encode report: %v\n", err)
			os.Exit(1)
		}
		return
	}
	rep.print()
}

func parseDelays(s string) ([]time.Duration, error) {
	parts := strings.Split(s, ",")
	out := make([]time.Duration, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("parse send delay %q: %w", p, err)
		}
		out = append(out, time.Duration(n)*time.Millisecond)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	if len(out) == 0 {
		return nil, fmt.Errorf("no send delays given")
	}
	return out, nil
}

func run(opt *options) (*report, error) {
	delays, err := parseDelays(opt.sendMS)
	if err != nil {
		return nil, err
	}

	svc, err := newServices(opt.pubURL)
	if err != nil {
		return nil, fmt.Errorf("connect services conn: %w", err)
	}
	defer svc.close()

	rep := &report{
		Topology:   opt.label,
		SubURL:     opt.subURL,
		PubURL:     opt.pubURL,
		Iterations: opt.iterations,
		RenderMS:   opt.renderMS,
	}

	rep.InterestWindow, err = measureInterestWindow(opt.subURL, svc.nc, 40)
	if err != nil {
		return nil, fmt.Errorf("measure interest window: %w", err)
	}

	for _, sc := range scenarios() {
		row := scenarioResult{Scenario: sc.name, Kind: string(sc.kind), Fix: sc.fix}
		for _, d := range delays {
			o, err := runScenario(opt, svc, sc, d)
			if err != nil {
				return nil, fmt.Errorf("scenario %s at %s: %w", sc.name, d, err)
			}
			o.SendMS = int(d / time.Millisecond)
			row.Points = append(row.Points, o)
		}
		rep.Scenarios = append(rep.Scenarios, row)
	}
	return rep, nil
}
