package dbschema

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	identifierPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	tableNamePattern  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$`)
)

func QualifyTable(name string, defaultSchema string) (string, error) {
	table := strings.TrimSpace(name)
	if table == "" {
		return "", fmt.Errorf("table is not configured")
	}
	if strings.Contains(table, ".") {
		if !tableNamePattern.MatchString(table) {
			return "", fmt.Errorf("invalid table name: [%s]", name)
		}
		return table, nil
	}
	if !identifierPattern.MatchString(table) {
		return "", fmt.Errorf("invalid table name: [%s]", name)
	}

	schema := strings.TrimSpace(defaultSchema)
	if schema == "" {
		return table, nil
	}
	if !identifierPattern.MatchString(schema) {
		return "", fmt.Errorf("invalid db schema: [%s]", defaultSchema)
	}

	return schema + "." + table, nil
}
