package integrations

import (
	"fmt"
	"strings"
)

type dbFieldRef struct {
	Table string
	Field string
}

type dbQueryAccessAnalysis struct {
	Tables map[string]struct{}
	Fields []dbFieldRef
}

type dbAccessControl struct {
	tableWhitelist map[string]struct{}
	tableDenylist  map[string]struct{}

	fieldWhitelistGlobal map[string]struct{}
	fieldWhitelistByTbl  map[string]map[string]struct{}
	fieldDenyGlobal      map[string]struct{}
	fieldDenyByTbl       map[string]map[string]struct{}

	hasTableWhitelist bool
	hasFieldWhitelist bool
	hasFieldDenylist  bool
}

func newDBAccessControl(cfg DatabaseConfig) dbAccessControl {
	ac := dbAccessControl{
		tableWhitelist:       normalizeIdentifierSet(cfg.TableWhitelist),
		tableDenylist:        normalizeIdentifierSet(cfg.TableDenylist),
		fieldWhitelistGlobal: make(map[string]struct{}),
		fieldWhitelistByTbl:  make(map[string]map[string]struct{}),
		fieldDenyGlobal:      make(map[string]struct{}),
		fieldDenyByTbl:       make(map[string]map[string]struct{}),
	}
	ac.hasTableWhitelist = len(ac.tableWhitelist) > 0
	loadFieldRules(cfg.FieldWhitelist, ac.fieldWhitelistGlobal, ac.fieldWhitelistByTbl)
	loadFieldRules(cfg.FieldDenylist, ac.fieldDenyGlobal, ac.fieldDenyByTbl)
	ac.hasFieldWhitelist = len(ac.fieldWhitelistGlobal) > 0 || len(ac.fieldWhitelistByTbl) > 0
	ac.hasFieldDenylist = len(ac.fieldDenyGlobal) > 0 || len(ac.fieldDenyByTbl) > 0
	return ac
}

func normalizeIdentifierSet(raw []string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, entry := range raw {
		norm := normalizeQualifiedIdentifier(entry)
		if norm == "" {
			continue
		}
		out[norm] = struct{}{}
	}
	return out
}

func loadFieldRules(raw []string, global map[string]struct{}, byTable map[string]map[string]struct{}) {
	for _, entry := range raw {
		table, field := parseFieldRule(entry)
		if field == "" {
			continue
		}
		if table == "" {
			global[field] = struct{}{}
			continue
		}
		if _, ok := byTable[table]; !ok {
			byTable[table] = make(map[string]struct{})
		}
		byTable[table][field] = struct{}{}
	}
}

func parseFieldRule(entry string) (table string, field string) {
	norm := normalizeQualifiedIdentifier(entry)
	if norm == "" {
		return "", ""
	}
	parts := strings.Split(norm, ".")
	if len(parts) == 1 {
		return "", parts[0]
	}
	return strings.Join(parts[:len(parts)-1], "."), parts[len(parts)-1]
}

func normalizeQualifiedIdentifier(input string) string {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return ""
	}
	parts := strings.Split(trimmed, ".")
	normParts := make([]string, 0, len(parts))
	for _, p := range parts {
		norm := normalizeIdentifierPart(p)
		if norm == "" {
			continue
		}
		normParts = append(normParts, norm)
	}
	return strings.Join(normParts, ".")
}

func normalizeIdentifierPart(part string) string {
	p := strings.TrimSpace(part)
	if p == "" {
		return ""
	}
	if len(p) >= 2 {
		switch {
		case strings.HasPrefix(p, "`") && strings.HasSuffix(p, "`"):
			p = p[1 : len(p)-1]
		case strings.HasPrefix(p, `"`) && strings.HasSuffix(p, `"`):
			p = p[1 : len(p)-1]
		case strings.HasPrefix(p, "[") && strings.HasSuffix(p, "]"):
			p = p[1 : len(p)-1]
		}
	}
	p = strings.TrimSpace(p)
	if p == "" {
		return ""
	}
	return strings.ToLower(p)
}

func identifierCandidates(identifier string) []string {
	norm := normalizeQualifiedIdentifier(identifier)
	if norm == "" {
		return nil
	}
	parts := strings.Split(norm, ".")
	if len(parts) == 1 {
		return []string{norm}
	}
	return []string{norm, parts[len(parts)-1]}
}

func (ac dbAccessControl) tableAllowed(tableName string) bool {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return true
	}
	candidates := identifierCandidates(tableName)
	if len(candidates) == 0 {
		return true
	}
	for _, candidate := range candidates {
		if _, denied := ac.tableDenylist[candidate]; denied {
			return false
		}
	}
	if !ac.hasTableWhitelist {
		return true
	}
	for _, candidate := range candidates {
		if _, ok := ac.tableWhitelist[candidate]; ok {
			return true
		}
	}
	return false
}

func (ac dbAccessControl) fieldAllowed(tableName, fieldName string) bool {
	field := normalizeIdentifierPart(fieldName)
	if field == "" {
		return false
	}
	tableCandidates := identifierCandidates(tableName)
	if ac.isFieldDenied(tableCandidates, field) {
		return false
	}
	if !ac.hasFieldWhitelist {
		return true
	}
	return ac.isFieldWhitelisted(tableCandidates, field)
}

func (ac dbAccessControl) isFieldDenied(tableCandidates []string, field string) bool {
	if _, denied := ac.fieldDenyGlobal["*"]; denied {
		return true
	}
	if _, denied := ac.fieldDenyGlobal[field]; denied {
		return true
	}
	for _, table := range tableCandidates {
		if cols, ok := ac.fieldDenyByTbl[table]; ok {
			if _, denied := cols["*"]; denied {
				return true
			}
			if _, denied := cols[field]; denied {
				return true
			}
		}
	}
	if len(tableCandidates) == 0 {
		for _, cols := range ac.fieldDenyByTbl {
			if _, denied := cols["*"]; denied {
				return true
			}
			if _, denied := cols[field]; denied {
				return true
			}
		}
	}
	return false
}

func (ac dbAccessControl) isFieldWhitelisted(tableCandidates []string, field string) bool {
	if _, ok := ac.fieldWhitelistGlobal["*"]; ok {
		return true
	}
	if _, ok := ac.fieldWhitelistGlobal[field]; ok {
		return true
	}
	for _, table := range tableCandidates {
		if cols, ok := ac.fieldWhitelistByTbl[table]; ok {
			if _, allowed := cols["*"]; allowed {
				return true
			}
			if _, allowed := cols[field]; allowed {
				return true
			}
		}
	}
	if len(tableCandidates) == 0 {
		for _, cols := range ac.fieldWhitelistByTbl {
			if _, allowed := cols["*"]; allowed {
				return true
			}
			if _, allowed := cols[field]; allowed {
				return true
			}
		}
	}
	return false
}

func (ac dbAccessControl) validateQuery(query string, enforceFieldRules bool) error {
	if !ac.hasTableWhitelist && len(ac.tableDenylist) == 0 && !ac.hasFieldWhitelist && !ac.hasFieldDenylist {
		return nil
	}
	analysis := analyzeQueryAccess(query)
	for table := range analysis.Tables {
		if !ac.tableAllowed(table) {
			return fmt.Errorf("access denied for table %q", table)
		}
	}
	if !enforceFieldRules {
		return nil
	}
	for _, ref := range analysis.Fields {
		if ref.Field == "*" {
			continue
		}
		if !ac.fieldAllowed(ref.Table, ref.Field) {
			if ref.Table != "" {
				return fmt.Errorf("access denied for field %q on table %q", ref.Field, ref.Table)
			}
			return fmt.Errorf("access denied for field %q", ref.Field)
		}
	}
	return nil
}

func (ac dbAccessControl) filterRows(rows []map[string]any, tableHint string) []map[string]any {
	if len(rows) == 0 {
		return rows
	}
	if !ac.hasFieldWhitelist && !ac.hasFieldDenylist {
		return rows
	}
	filteredRows := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		filtered := make(map[string]any)
		for key, value := range row {
			if ac.fieldAllowed(tableHint, key) {
				filtered[key] = value
			}
		}
		filteredRows = append(filteredRows, filtered)
	}
	return filteredRows
}

func singleTableHint(tables map[string]struct{}) string {
	if len(tables) != 1 {
		return ""
	}
	for table := range tables {
		return table
	}
	return ""
}

func analyzeQueryAccess(query string) dbQueryAccessAnalysis {
	tokens := tokenizeSQL(query)
	ctes := extractCTENames(tokens)
	tables, aliases := extractTablesAndAliases(tokens, ctes)
	fields := extractFieldReferences(tokens, tables, aliases, ctes)
	return dbQueryAccessAnalysis{Tables: tables, Fields: fields}
}

type sqlTokenKind int

const (
	tokenIdentifier sqlTokenKind = iota
	tokenSymbol
	tokenKeyword
)

type sqlToken struct {
	kind  sqlTokenKind
	text  string
	lower string
}

var sqlKeywords = map[string]struct{}{
	"all": {}, "and": {}, "any": {}, "as": {}, "asc": {}, "between": {}, "by": {}, "case": {}, "create": {},
	"cross": {}, "cte": {}, "current": {}, "default": {}, "delete": {}, "desc": {}, "distinct": {}, "drop": {},
	"else": {}, "end": {}, "except": {}, "exists": {}, "false": {}, "fetch": {}, "from": {}, "full": {},
	"group": {}, "having": {}, "in": {}, "inner": {}, "insert": {}, "intersect": {}, "into": {}, "is": {},
	"join": {}, "left": {}, "like": {}, "limit": {}, "lateral": {}, "natural": {}, "not": {}, "null": {},
	"offset": {}, "on": {}, "only": {}, "or": {}, "order": {}, "outer": {}, "over": {}, "recursive": {},
	"replace": {}, "returning": {}, "right": {}, "select": {}, "set": {}, "show": {}, "table": {}, "then": {},
	"top": {}, "true": {}, "truncate": {}, "union": {}, "update": {}, "using": {}, "values": {}, "when": {},
	"where": {}, "window": {}, "with": {}, "explain": {}, "pragma": {}, "database": {},
}

func tokenizeSQL(query string) []sqlToken {
	tokens := make([]sqlToken, 0, 128)
	for i := 0; i < len(query); {
		ch := query[i]
		if isSQLWhitespace(ch) {
			i++
			continue
		}
		if ch == '-' && i+1 < len(query) && query[i+1] == '-' {
			i += 2
			for i < len(query) && query[i] != '\n' {
				i++
			}
			continue
		}
		if ch == '/' && i+1 < len(query) && query[i+1] == '*' {
			i += 2
			for i+1 < len(query) && !(query[i] == '*' && query[i+1] == '/') {
				i++
			}
			if i+1 < len(query) {
				i += 2
			}
			continue
		}
		if ch == '\'' {
			i++
			for i < len(query) {
				if query[i] == '\'' {
					if i+1 < len(query) && query[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}
		if ch == '$' && i+1 < len(query) && query[i+1] == '$' {
			i += 2
			for i+1 < len(query) && !(query[i] == '$' && query[i+1] == '$') {
				i++
			}
			if i+1 < len(query) {
				i += 2
			}
			continue
		}
		if ch == '`' || ch == '"' || ch == '[' {
			end := ch
			if ch == '[' {
				end = ']'
			}
			i++
			start := i
			for i < len(query) {
				if query[i] == end {
					if end == '"' && i+1 < len(query) && query[i+1] == '"' {
						i += 2
						continue
					}
					break
				}
				i++
			}
			ident := query[start:i]
			tokens = append(tokens, sqlToken{kind: tokenIdentifier, text: ident, lower: strings.ToLower(strings.TrimSpace(ident))})
			if i < len(query) {
				i++
			}
			continue
		}
		if isSQLIdentifierStart(ch) {
			start := i
			i++
			for i < len(query) && isSQLIdentifierPart(query[i]) {
				i++
			}
			word := query[start:i]
			lower := strings.ToLower(word)
			kind := tokenIdentifier
			if _, ok := sqlKeywords[lower]; ok {
				kind = tokenKeyword
			}
			tokens = append(tokens, sqlToken{kind: kind, text: word, lower: lower})
			continue
		}
		if strings.ContainsRune("(),.=*;+/-", rune(ch)) {
			tokens = append(tokens, sqlToken{kind: tokenSymbol, text: string(ch), lower: string(ch)})
			i++
			continue
		}
		i++
	}
	return tokens
}

func isSQLWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f'
}

func isSQLIdentifierStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || ch == '$'
}

func isSQLIdentifierPart(ch byte) bool {
	return isSQLIdentifierStart(ch) || (ch >= '0' && ch <= '9') || ch == '$'
}

func extractCTENames(tokens []sqlToken) map[string]struct{} {
	ctes := make(map[string]struct{})
	if len(tokens) == 0 || tokens[0].lower != "with" {
		return ctes
	}
	i := 1
	if i < len(tokens) && tokens[i].lower == "recursive" {
		i++
	}
	for i < len(tokens) {
		if tokens[i].kind != tokenIdentifier {
			break
		}
		name := normalizeQualifiedIdentifier(tokens[i].text)
		if name != "" {
			ctes[name] = struct{}{}
		}
		i++
		if i < len(tokens) && tokens[i].text == "(" {
			i = skipBalancedParentheses(tokens, i)
		}
		if i >= len(tokens) || tokens[i].lower != "as" {
			break
		}
		i++
		if i >= len(tokens) || tokens[i].text != "(" {
			break
		}
		i = skipBalancedParentheses(tokens, i)
		if i >= len(tokens) || tokens[i].text != "," {
			break
		}
		i++
	}
	return ctes
}

func skipBalancedParentheses(tokens []sqlToken, start int) int {
	if start >= len(tokens) || tokens[start].text != "(" {
		return start
	}
	depth := 0
	for i := start; i < len(tokens); i++ {
		switch tokens[i].text {
		case "(":
			depth++
		case ")":
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return len(tokens)
}

func extractTablesAndAliases(tokens []sqlToken, ctes map[string]struct{}) (map[string]struct{}, map[string]string) {
	tables := make(map[string]struct{})
	aliases := make(map[string]string)
	for i := 0; i < len(tokens); i++ {
		kw := tokens[i].lower
		switch kw {
		case "from":
			next := addTableFactor(tokens, i+1, tables, aliases, ctes)
			for next < len(tokens) && tokens[next].text == "," {
				next = addTableFactor(tokens, next+1, tables, aliases, ctes)
			}
			i = next - 1
		case "join":
			i = addTableFactor(tokens, i+1, tables, aliases, ctes) - 1
		case "update":
			i = addTableFactor(tokens, i+1, tables, aliases, ctes) - 1
		case "insert":
			if i+1 < len(tokens) && tokens[i+1].lower == "into" {
				i = addTableFactor(tokens, i+2, tables, aliases, ctes) - 1
			}
		case "delete":
			if i+1 < len(tokens) && tokens[i+1].lower == "from" {
				i = addTableFactor(tokens, i+2, tables, aliases, ctes) - 1
			}
		case "truncate":
			start := i + 1
			if start < len(tokens) && tokens[start].lower == "table" {
				start++
			}
			i = addTableFactor(tokens, start, tables, aliases, ctes) - 1
		}
	}
	for table := range tables {
		aliases[table] = table
		parts := strings.Split(table, ".")
		aliases[parts[len(parts)-1]] = table
	}
	return tables, aliases
}

func addTableFactor(tokens []sqlToken, start int, tables map[string]struct{}, aliases map[string]string, ctes map[string]struct{}) int {
	i := start
	for i < len(tokens) {
		if tokens[i].text == "(" {
			i = skipBalancedParentheses(tokens, i)
			if i < len(tokens) && tokens[i].lower == "as" {
				i++
				if i < len(tokens) && tokens[i].kind == tokenIdentifier {
					i++
				}
			} else if i < len(tokens) && tokens[i].kind == tokenIdentifier {
				i++
			}
			return i
		}
		if tokens[i].kind == tokenIdentifier {
			break
		}
		i++
	}
	if i >= len(tokens) || tokens[i].kind != tokenIdentifier {
		return i
	}
	segments, next := readQualifiedSegments(tokens, i)
	if len(segments) == 0 {
		return next
	}
	tableName := strings.Join(segments, ".")
	if _, isCTE := ctes[tableName]; !isCTE {
		tables[tableName] = struct{}{}
	}
	aliasIdx := next
	if aliasIdx < len(tokens) && tokens[aliasIdx].lower == "as" {
		aliasIdx++
	}
	if aliasIdx < len(tokens) && tokens[aliasIdx].kind == tokenIdentifier {
		alias := normalizeQualifiedIdentifier(tokens[aliasIdx].text)
		if alias != "" {
			if _, isKeyword := sqlKeywords[alias]; !isKeyword {
				aliases[alias] = tableName
			}
		}
		aliasIdx++
	}
	return aliasIdx
}

func readQualifiedSegments(tokens []sqlToken, start int) ([]string, int) {
	if start >= len(tokens) || tokens[start].kind != tokenIdentifier {
		return nil, start
	}
	segments := []string{normalizeIdentifierPart(tokens[start].text)}
	i := start + 1
	for i+1 < len(tokens) && tokens[i].text == "." && tokens[i+1].kind == tokenIdentifier {
		segments = append(segments, normalizeIdentifierPart(tokens[i+1].text))
		i += 2
	}
	clean := segments[:0]
	for _, seg := range segments {
		if seg != "" {
			clean = append(clean, seg)
		}
	}
	return clean, i
}

func extractFieldReferences(tokens []sqlToken, tables map[string]struct{}, aliases map[string]string, ctes map[string]struct{}) []dbFieldRef {
	refs := make([]dbFieldRef, 0, 16)
	seen := make(map[string]struct{})
	selectAliases := extractSelectAliases(tokens)

	defaultTable := singleTableHint(tables)

	add := func(table, field string) {
		table = normalizeQualifiedIdentifier(table)
		field = normalizeIdentifierPart(field)
		if field == "" {
			return
		}
		if table != "" {
			if mapped, ok := aliases[table]; ok {
				table = mapped
			}
		}
		if table == "" {
			table = defaultTable
		}
		key := table + "|" + field
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		refs = append(refs, dbFieldRef{Table: table, Field: field})
	}

	for i := 0; i < len(tokens); i++ {
		if tokens[i].kind != tokenIdentifier {
			continue
		}
		segments, next := readQualifiedSegments(tokens, i)
		if len(segments) >= 2 {
			table := strings.Join(segments[:len(segments)-1], ".")
			field := segments[len(segments)-1]
			if _, isCTE := ctes[table]; !isCTE {
				add(table, field)
			}
			i = next - 1
		}
	}

	for i := 0; i < len(tokens); i++ {
		if tokens[i].lower != "insert" || i+1 >= len(tokens) || tokens[i+1].lower != "into" {
			continue
		}
		segments, next := readQualifiedSegments(tokens, i+2)
		if len(segments) == 0 {
			continue
		}
		table := strings.Join(segments, ".")
		i = next
		if i >= len(tokens) || tokens[i].text != "(" {
			continue
		}
		for i < len(tokens) && tokens[i].text != ")" {
			i++
			if i < len(tokens) && tokens[i].kind == tokenIdentifier {
				add(table, tokens[i].text)
			}
		}
	}

	for i := 0; i < len(tokens); i++ {
		if tokens[i].lower != "update" {
			continue
		}
		segments, next := readQualifiedSegments(tokens, i+1)
		if len(segments) == 0 {
			continue
		}
		table := strings.Join(segments, ".")
		i = next
		for i < len(tokens) && tokens[i].lower != "set" {
			i++
		}
		if i >= len(tokens) {
			continue
		}
		i++
		for i < len(tokens) {
			if tokens[i].lower == "where" || tokens[i].lower == "returning" || tokens[i].lower == "from" {
				break
			}
			if tokens[i].kind == tokenIdentifier {
				colSegments, colEnd := readQualifiedSegments(tokens, i)
				if len(colSegments) > 0 {
					if colEnd < len(tokens) && tokens[colEnd].text == "=" {
						if len(colSegments) == 1 {
							add(table, colSegments[0])
						} else {
							add(strings.Join(colSegments[:len(colSegments)-1], "."), colSegments[len(colSegments)-1])
						}
					}
					i = colEnd
					continue
				}
			}
			i++
		}
	}

	for i := 0; i < len(tokens); i++ {
		if tokens[i].lower != "select" {
			continue
		}
		depth := 0
		j := i + 1
		start := j
		for j < len(tokens) {
			if tokens[j].text == "(" {
				depth++
			} else if tokens[j].text == ")" {
				if depth > 0 {
					depth--
				}
			} else if depth == 0 && tokens[j].lower == "from" {
				break
			}
			j++
		}
		if start < j {
			extractFieldsFromSelectList(tokens[start:j], aliases, tables, add)
		}
		i = j - 1
	}

	for i := 0; i < len(tokens); i++ {
		if tokens[i].kind != tokenIdentifier {
			continue
		}
		if i+1 < len(tokens) && tokens[i+1].text == "(" {
			continue
		}
		if i > 0 && tokens[i-1].text == "." {
			continue
		}
		if i+1 < len(tokens) && tokens[i+1].text == "." {
			continue
		}
		candidate := normalizeIdentifierPart(tokens[i].text)
		if candidate == "" {
			continue
		}
		if _, keyword := sqlKeywords[candidate]; keyword {
			continue
		}
		if _, alias := aliases[candidate]; alias {
			continue
		}
		if _, cte := ctes[candidate]; cte {
			continue
		}
		if _, tableName := tables[candidate]; tableName {
			continue
		}
		if _, projectedAlias := selectAliases[candidate]; projectedAlias {
			continue
		}
		if i > 0 {
			prev := tokens[i-1].lower
			if prev == "from" || prev == "join" || prev == "update" || prev == "into" || prev == "as" || prev == "table" || prev == "truncate" || prev == "delete" || prev == "insert" {
				continue
			}
		}
		add("", candidate)
	}

	return refs
}

func extractSelectAliases(tokens []sqlToken) map[string]struct{} {
	aliases := make(map[string]struct{})
	for i := 0; i < len(tokens); i++ {
		if tokens[i].lower != "select" {
			continue
		}
		depth := 0
		j := i + 1
		start := j
		for j < len(tokens) {
			if tokens[j].text == "(" {
				depth++
			} else if tokens[j].text == ")" {
				if depth > 0 {
					depth--
				}
			} else if depth == 0 && tokens[j].lower == "from" {
				break
			}
			j++
		}
		if start < j {
			for _, expr := range splitByTopLevelComma(tokens[start:j]) {
				if alias := expressionAlias(expr); alias != "" {
					aliases[alias] = struct{}{}
				}
			}
		}
		i = j - 1
	}
	return aliases
}

func extractFieldsFromSelectList(tokens []sqlToken, aliases map[string]string, tables map[string]struct{}, add func(table, field string)) {
	segments := splitByTopLevelComma(tokens)
	for _, expr := range segments {
		if len(expr) == 0 {
			continue
		}
		expr = stripExpressionAlias(expr)
		if len(expr) == 0 {
			continue
		}
		for i := 0; i < len(expr); i++ {
			if expr[i].text == "*" {
				if i >= 2 && expr[i-1].text == "." && expr[i-2].kind == tokenIdentifier {
					add(expr[i-2].text, "*")
				} else {
					add("", "*")
				}
				continue
			}
			if expr[i].kind != tokenIdentifier {
				continue
			}
			if i+1 < len(expr) && expr[i+1].text == "(" {
				continue
			}
			if i > 0 && expr[i-1].text == "." {
				continue
			}
			if i+1 < len(expr) && expr[i+1].text == "." {
				continue
			}
			candidate := normalizeIdentifierPart(expr[i].text)
			if candidate == "" {
				continue
			}
			if _, keyword := sqlKeywords[candidate]; keyword {
				continue
			}
			if _, tableAlias := aliases[candidate]; tableAlias {
				continue
			}
			if _, tableName := tables[candidate]; tableName {
				continue
			}
			add("", candidate)
		}
	}
}

func splitByTopLevelComma(tokens []sqlToken) [][]sqlToken {
	segments := make([][]sqlToken, 0)
	start := 0
	depth := 0
	for i := 0; i < len(tokens); i++ {
		switch tokens[i].text {
		case "(":
			depth++
		case ")":
			if depth > 0 {
				depth--
			}
		case ",":
			if depth == 0 {
				segments = append(segments, tokens[start:i])
				start = i + 1
			}
		}
	}
	segments = append(segments, tokens[start:])
	return segments
}

func stripExpressionAlias(tokens []sqlToken) []sqlToken {
	depth := 0
	for i := len(tokens) - 1; i >= 0; i-- {
		switch tokens[i].text {
		case ")":
			depth++
		case "(":
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && tokens[i].lower == "as" {
			return tokens[:i]
		}
	}
	if len(tokens) >= 2 && tokens[len(tokens)-1].kind == tokenIdentifier {
		prev := tokens[len(tokens)-2]
		if prev.kind == tokenIdentifier || prev.text == ")" || prev.text == "*" {
			return tokens[:len(tokens)-1]
		}
	}
	return tokens
}

func expressionAlias(tokens []sqlToken) string {
	if len(tokens) == 0 {
		return ""
	}
	depth := 0
	for i := len(tokens) - 1; i >= 0; i-- {
		switch tokens[i].text {
		case ")":
			depth++
		case "(":
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && tokens[i].lower == "as" && i+1 < len(tokens) && tokens[i+1].kind == tokenIdentifier {
			return normalizeIdentifierPart(tokens[i+1].text)
		}
	}
	if len(tokens) >= 2 && tokens[len(tokens)-1].kind == tokenIdentifier {
		prev := tokens[len(tokens)-2]
		if prev.kind == tokenIdentifier || prev.text == ")" || prev.text == "*" {
			return normalizeIdentifierPart(tokens[len(tokens)-1].text)
		}
	}
	return ""
}
