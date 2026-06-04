package main

import (
	"fmt"
	"io"
	"strings"
)

// renderBottleneck writes the BOTTLENECK: block. For an undetermined verdict it
// writes a single line naming why; otherwise it names the culprit, the causal
// reasons, and the confidence.
func renderBottleneck(w io.Writer, v *bottleneckVerdict) {
	if !v.Determined {
		reason := "no signal"
		if len(v.Reasons) > 0 {
			reason = v.Reasons[0]
		}
		fmt.Fprintf(w, "BOTTLENECK: undetermined (%s)\n", reason)
		return
	}
	fmt.Fprintf(w, "BOTTLENECK: %s (%s-bound)\n", v.Component, v.Resource)
	for _, r := range v.Reasons {
		fmt.Fprintf(w, "        %s\n", r)
	}
	fmt.Fprintf(w, "        confidence: %s\n", v.Confidence)
}

// bottleneckCSVColumns returns the trip-row culprit columns appended to the CSV.
func bottleneckCSVColumns(v *bottleneckVerdict) []string {
	if !v.Determined {
		return []string{"undetermined", "", ""}
	}
	return []string{v.Component, strings.ToLower(v.Resource), v.Confidence}
}
