import React, { useRef, useEffect, useState, useMemo } from 'react';
import Editor, { Monaco } from '@monaco-editor/react';
import { api } from '@/lib/api';
import { Integration } from '@/types/etl';

interface SQLEditorProps {
    value: string;
    onChange: (value: string) => void;
    onExecute?: () => void;
    height?: string;
    readOnly?: boolean;
}

export default function SQLEditor({
    value,
    onChange,
    onExecute,
    height = '400px',
    readOnly = false
}: SQLEditorProps) {
    const editorRef = useRef<any>(null);
    const monacoRef = useRef<Monaco | null>(null);
    const [integrations, setIntegrations] = useState<Integration[]>([]);
    const [schemaCache, setSchemaCache] = useState<Record<string, any>>({});

    useEffect(() => {
        // Load integrations for autocomplete
        api.getIntegrations().then(setIntegrations).catch(console.error);
    }, []);

    const loadSchemaForIntegration = async (integrationName: string) => {
        if (schemaCache[integrationName]) {
            return schemaCache[integrationName];
        }

        try {
            const schema = await api.getSchema(integrationName);
            setSchemaCache(prev => ({ ...prev, [integrationName]: schema }));
            return schema;
        } catch (error) {
            console.error('Failed to load schema:', error);
            return null;
        }
    };

    const handleEditorDidMount = (editor: any, monaco: Monaco) => {
        editorRef.current = editor;
        monacoRef.current = monaco;

        // Configure Monaco for SQL
        monaco.languages.setMonarchTokensProvider('sql', {
            tokenizer: {
                root: [
                    // Keywords
                    [/\b(SELECT|FROM|WHERE|JOIN|INNER|LEFT|RIGHT|OUTER|ON|GROUP|BY|HAVING|ORDER|LIMIT|OFFSET|UNION|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TABLE|INDEX|VIEW)\b/i, 'keyword'],
                    // Functions
                    [/\b(COUNT|SUM|AVG|MIN|MAX|UPPER|LOWER|LENGTH|SUBSTRING|CONCAT|TRIM|ROUND|NOW|DATE|TIME)\b/i, 'predefined'],
                    // Read functions
                    [/\b(read_file|read_db|read_service)\b/i, 'predefined'],
                    // Strings
                    [/'([^'\\]|\\.)*$/, 'string.invalid'],
                    [/'([^'\\]|\\.)*'/, 'string'],
                    [/"/, { token: 'string.quote', bracket: '@open', next: '@string_double' }],
                    [/'/, { token: 'string.quote', bracket: '@open', next: '@string_single' }],
                    // Comments
                    [/--.*$/, 'comment'],
                    [/#.*$/, 'comment'],
                    // Numbers
                    [/\d+/, 'number'],
                    // Operators
                    [/[<>=!%&|+\-*\/]+/, 'operator'],
                ],
                string_double: [
                    [/[^\\"]+/, 'string'],
                    [/\\./, 'string.escape'],
                    [/"/, { token: 'string.quote', bracket: '@close', next: '@pop' }],
                ],
                string_single: [
                    [/[^\\']+/, 'string'],
                    [/\\./, 'string.escape'],
                    [/'/, { token: 'string.quote', bracket: '@close', next: '@pop' }],
                ],
            },
        });

        // Register completion provider
        monaco.languages.registerCompletionItemProvider('sql', {
            provideCompletionItems: async (model: any, position: any) => {
                const word = model.getWordUntilPosition(position);
                const range = {
                    startLineNumber: position.lineNumber,
                    endLineNumber: position.lineNumber,
                    startColumn: word.startColumn,
                    endColumn: word.endColumn,
                };

                const suggestions: any[] = [
                    // SQL Keywords
                    { label: 'SELECT', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'SELECT ', detail: 'SQL SELECT statement', range },
                    { label: 'FROM', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'FROM ', detail: 'SQL FROM clause', range },
                    { label: 'WHERE', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'WHERE ', detail: 'SQL WHERE clause', range },
                    { label: 'JOIN', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'JOIN ', detail: 'SQL JOIN clause', range },
                    { label: 'GROUP BY', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'GROUP BY ', detail: 'SQL GROUP BY clause', range },
                    { label: 'ORDER BY', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'ORDER BY ', detail: 'SQL ORDER BY clause', range },
                    { label: 'LIMIT', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'LIMIT ', detail: 'SQL LIMIT clause', range },

                    // Functions
                    { label: 'COUNT', kind: monaco.languages.CompletionItemKind.Function, insertText: 'COUNT(${1:*})', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, detail: 'Count function', range },
                    { label: 'SUM', kind: monaco.languages.CompletionItemKind.Function, insertText: 'SUM(${1:column})', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, detail: 'Sum function', range },
                    { label: 'AVG', kind: monaco.languages.CompletionItemKind.Function, insertText: 'AVG(${1:column})', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, detail: 'Average function', range },
                    { label: 'MIN', kind: monaco.languages.CompletionItemKind.Function, insertText: 'MIN(${1:column})', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, detail: 'Minimum function', range },
                    { label: 'MAX', kind: monaco.languages.CompletionItemKind.Function, insertText: 'MAX(${1:column})', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, detail: 'Maximum function', range },

                    // Read functions
                    { label: 'read_file', kind: monaco.languages.CompletionItemKind.Function, insertText: "read_file('${1:filename}')", insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, detail: 'Read data from file', range },
                    { label: 'read_db', kind: monaco.languages.CompletionItemKind.Function, insertText: "read_db('${1:connection}', '${2:table}')", insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, detail: 'Read data from database', range },
                    { label: 'read_service', kind: monaco.languages.CompletionItemKind.Function, insertText: "read_service('${1:service}', '${2:endpoint}')", insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, detail: 'Read data from service', range },
                ];

                // Add integration names
                integrations.forEach(integration => {
                    suggestions.push({
                        label: integration.name,
                        kind: monaco.languages.CompletionItemKind.Value,
                        insertText: `'${integration.name}'`,
                        detail: `${integration.type} integration`,
                        range,
                    });
                });

                // Check if we're inside a read function to provide schema suggestions
                const textUntilPosition = model.getValueInRange({
                    startLineNumber: 1,
                    startColumn: 1,
                    endLineNumber: position.lineNumber,
                    endColumn: position.column,
                });

                // Check for read_file, read_db, read_service patterns
                const readFileMatch = textUntilPosition.match(/read_file\(['"]([^'"]*)$/);
                const readDbMatch = textUntilPosition.match(/read_db\(['"]([^'"]*)['"]\s*,\s*['"]([^'"]*)$/);
                const readServiceMatch = textUntilPosition.match(/read_service\(['"]([^'"]*)['"]\s*,\s*['"]([^'"]*)$/);

                if (readFileMatch || readDbMatch || readServiceMatch) {
                    let integrationName = '';
                    let isTableContext = false;

                    if (readFileMatch) {
                        integrationName = readFileMatch[1];
                    } else if (readDbMatch) {
                        integrationName = readDbMatch[1];
                        isTableContext = readDbMatch[2] === '';
                    } else if (readServiceMatch) {
                        integrationName = readServiceMatch[1];
                        isTableContext = readServiceMatch[2] === '';
                    }

                    if (integrationName) {
                        const schema = await loadSchemaForIntegration(integrationName);
                        if (schema) {
                            if (isTableContext) {
                                // Suggest table names
                                schema.tables.forEach((table: string) => {
                                    suggestions.push({
                                        label: table,
                                        kind: monaco.languages.CompletionItemKind.Value,
                                        insertText: `'${table}'`,
                                        detail: `Table from ${integrationName}`,
                                        range,
                                    });
                                });
                            } else {
                                // Suggest column names if we have a table context
                                const tableMatch = textUntilPosition.match(/read_db\('[^']+'\s*,\s*'([^']*)'/);
                                if (tableMatch && schema.columns[tableMatch[1]]) {
                                    schema.columns[tableMatch[1]].forEach((column: string) => {
                                        suggestions.push({
                                            label: column,
                                            kind: monaco.languages.CompletionItemKind.Field,
                                            insertText: column,
                                            detail: `Column from ${tableMatch[1]}`,
                                            range,
                                        });
                                    });
                                }
                            }
                        }
                    }
                }

                return { suggestions };
            },
        });

        // Add keyboard shortcuts
        editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
            if (onExecute) {
                onExecute();
            }
        });

        editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, () => {
            // Save query (could be extended to save to backend)
            console.log('Save shortcut pressed');
        });

        editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.KeyF, () => {
            // Format query
            editor.getAction('editor.action.formatDocument').run();
        });

        editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Slash, () => {
            // Toggle comment
            editor.getAction('editor.action.commentLine').run();
        });

        // Configure editor options with enhanced features
        editor.updateOptions({
            fontSize: 14,
            lineNumbers: 'on',
            roundedSelection: false,
            scrollBeyondLastLine: false,
            minimap: { enabled: false },
            wordWrap: 'on',
            suggestOnTriggerCharacters: true,
            acceptSuggestionOnEnter: 'on',
            tabCompletion: 'on',
            multiCursorModifier: 'ctrlCmd',
            selectionHighlight: true,
            occurrencesHighlight: 'singleFile',
            codeLens: false,
            folding: true,
            foldingHighlight: true,
            showFoldingControls: 'mouseover',
            unfoldOnClickAfterEndOfLine: true,
            renderWhitespace: 'selection',
            renderControlCharacters: false,
            renderLineHighlight: 'line',
            automaticLayout: true,
            bracketPairColorization: { enabled: true },
            guides: {
                bracketPairs: true,
                indentation: true,
            },
            // Enable multiple cursors
            multiCursorLimit: 10000,
            // Better find widget
            find: {
                addExtraSpaceOnTop: false,
                autoFindInSelection: 'never',
                seedSearchStringFromSelection: 'always',
            },
        });
    };

    const handleValidation = async (markers: any[]) => {
        if (value.trim()) {
            try {
                const response = await api.validateQuery(value);
                if (monacoRef.current && editorRef.current) {
                    const monaco = monacoRef.current;
                    const model = editorRef.current.getModel();
                    if (model) {
                        const monacoMarkers = response.errors.map((error: string, index: number) => ({
                            startLineNumber: 1,
                            startColumn: 1,
                            endLineNumber: 1,
                            endColumn: model.getLineLength(1) + 1,
                            message: error,
                            severity: monaco.MarkerSeverity.Error,
                        }));
                        monaco.editor.setModelMarkers(model, 'sql', monacoMarkers);
                    }
                }
            } catch (error) {
                console.error('Validation error:', error);
            }
        } else {
            // Clear markers if query is empty
            if (monacoRef.current && editorRef.current) {
                const monaco = monacoRef.current;
                const model = editorRef.current.getModel();
                if (model) {
                    monaco.editor.setModelMarkers(model, 'sql', []);
                }
            }
        }
    };

    // Debounced validation
    const debouncedValidation = useMemo(() => {
        let timeoutId: NodeJS.Timeout;
        return () => {
            clearTimeout(timeoutId);
            timeoutId = setTimeout(() => {
                handleValidation([]);
            }, 500);
        };
    }, [value]);

    useEffect(() => {
        debouncedValidation();
    }, [value, debouncedValidation]);

    return (
        <div className="border border-border rounded-md overflow-hidden">
            <Editor
                height={height}
                language="sql"
                value={value}
                onChange={(value) => onChange(value || '')}
                onMount={handleEditorDidMount}
                onValidate={handleValidation}
                options={{
                    readOnly,
                    theme: 'vs-light',
                    fontFamily: 'JetBrains Mono, Consolas, monospace',
                    fontLigatures: true,
                    cursorBlinking: 'smooth',
                    cursorSmoothCaretAnimation: 'on',
                    smoothScrolling: true,
                    contextmenu: true,
                    mouseWheelZoom: true,
                    multiCursorModifier: 'ctrlCmd',
                    selectionHighlight: true,
                    occurrencesHighlight: 'singleFile',
                    codeLens: false,
                    folding: true,
                    foldingHighlight: true,
                    showFoldingControls: 'mouseover',
                    unfoldOnClickAfterEndOfLine: true,
                    renderWhitespace: 'selection',
                    renderControlCharacters: false,
                    renderLineHighlight: 'line',
                    scrollBeyondLastLine: false,
                    automaticLayout: true,
                }}
            />
        </div>
    );
}
