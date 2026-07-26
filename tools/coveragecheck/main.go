package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"strconv"
	"strings"
)

type coverageStats struct {
	covered int
	total   int
}

func (s coverageStats) percent() float64 {
	if s.total == 0 {
		return 0
	}
	return float64(s.covered) * 100 / float64(s.total)
}

func (s coverageStats) meets(minimum float64) bool {
	return s.percent() >= minimum
}

func readCoverageProfile(
	reader io.Reader,
	include string,
	excludes map[string]struct{},
) (coverageStats, error) {
	if include == "" {
		return coverageStats{}, fmt.Errorf("coverage include pattern is required")
	}
	scanner := bufio.NewScanner(reader)
	if !scanner.Scan() {
		return coverageStats{}, fmt.Errorf("coverage profile is empty")
	}
	if !strings.HasPrefix(scanner.Text(), "mode: ") {
		return coverageStats{}, fmt.Errorf("coverage profile has no mode header")
	}

	var stats coverageStats
	lineNumber := 1
	for scanner.Scan() {
		lineNumber++
		fields := strings.Fields(scanner.Text())
		if len(fields) != 3 {
			return coverageStats{}, fmt.Errorf(
				"parse coverage profile line %d: expected three fields",
				lineNumber,
			)
		}
		separator := strings.LastIndex(fields[0], ":")
		if separator < 0 {
			return coverageStats{}, fmt.Errorf(
				"parse coverage profile line %d: missing source range",
				lineNumber,
			)
		}
		source := strings.ReplaceAll(fields[0][:separator], "\\", "/")
		if !strings.Contains(source, include) {
			continue
		}
		if _, excluded := excludes[path.Base(source)]; excluded {
			continue
		}
		statements, err := strconv.Atoi(fields[1])
		if err != nil || statements < 0 {
			return coverageStats{}, fmt.Errorf(
				"parse coverage profile line %d statement count %q",
				lineNumber,
				fields[1],
			)
		}
		count, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil || count < 0 {
			return coverageStats{}, fmt.Errorf(
				"parse coverage profile line %d execution count %q",
				lineNumber,
				fields[2],
			)
		}
		stats.total += statements
		if count > 0 {
			stats.covered += statements
		}
	}
	if err := scanner.Err(); err != nil {
		return coverageStats{}, fmt.Errorf("read coverage profile: %w", err)
	}
	if stats.total == 0 {
		return coverageStats{}, fmt.Errorf(
			"coverage include pattern %q matched no statements",
			include,
		)
	}
	return stats, nil
}

type stringSetFlag map[string]struct{}

func (f stringSetFlag) String() string {
	values := make([]string, 0, len(f))
	for value := range f {
		values = append(values, value)
	}
	return strings.Join(values, ",")
}

func (f stringSetFlag) Set(value string) error {
	if value == "" {
		return errors.New("exclude filename cannot be empty")
	}
	f[value] = struct{}{}
	return nil
}

func run(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("coveragecheck", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	profilePath := fs.String("profile", "", "Go coverage profile")
	include := fs.String("include", "", "source path substring to include")
	minimum := fs.Float64("min", 0, "minimum statement coverage percentage")
	excludes := make(stringSetFlag)
	fs.Var(excludes, "exclude", "source base filename to exclude; repeatable")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse coverage arguments: %w", err)
	}
	if *profilePath == "" {
		return fmt.Errorf("coverage profile path is required")
	}
	if *minimum < 0 || *minimum > 100 {
		return fmt.Errorf("minimum coverage must be between 0 and 100")
	}

	file, err := os.Open(*profilePath)
	if err != nil {
		return fmt.Errorf("open coverage profile: %w", err)
	}
	defer func() { _ = file.Close() }()

	stats, err := readCoverageProfile(file, *include, excludes)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(
		stdout,
		"coverage include=%q covered=%d total=%d percent=%.2f required=%.2f\n",
		*include,
		stats.covered,
		stats.total,
		stats.percent(),
		*minimum,
	)
	if err != nil {
		return fmt.Errorf("write coverage result: %w", err)
	}
	if !stats.meets(*minimum) {
		return fmt.Errorf(
			"coverage %.2f%% is below required %.2f%%",
			stats.percent(),
			*minimum,
		)
	}
	return nil
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		slog.Error("coverage check failed", "error", err)
		os.Exit(1)
	}
}
