package main

import (
	"fmt"
	"strings"
)

// identityResolver maps a logical service name to a cAdvisor PromQL label
// selector. The fallback map (name -> 12-char container short-ID) is used on
// hosts where cAdvisor cannot populate the compose-service label.
type identityResolver struct {
	fallback map[string]string
}

// parseContainerMap parses "shortid:name,shortid2:name2" into a name->shortid
// map. An empty string yields an empty map.
func parseContainerMap(s string) (map[string]string, error) {
	out := map[string]string{}
	if strings.TrimSpace(s) == "" {
		return out, nil
	}
	for _, pair := range strings.Split(s, ",") {
		id, name, ok := strings.Cut(strings.TrimSpace(pair), ":")
		if !ok || id == "" || name == "" {
			return nil, fmt.Errorf("bad container-map entry %q (want shortid:name)", pair)
		}
		out[name] = id
	}
	return out, nil
}

// selector returns the inner PromQL label matcher (no metric name, no braces)
// that identifies the given service's container.
func (r identityResolver) selector(service string) string {
	if id, ok := r.fallback[service]; ok {
		return fmt.Sprintf(`id=~".*%s.*"`, id)
	}
	return fmt.Sprintf(`container_label_com_docker_compose_service=%q`, service)
}
