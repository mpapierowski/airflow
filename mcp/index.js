#!/usr/bin/env node

import { McpServer, ResourceTemplate } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

// Create an MCP server
const server = new McpServer({
    name: "demo-airflow-mcp-server",
    version: "1.0.0"
});

// Add an addition tool
server.registerTool("parse_bundle",
    {
        title: "Parse and validate Airflow DAG bundle",
        description: `Use this tool to parse and validate an Airflow DAG bundle. 
        
        Arguments:
          - bundle: Name of the bundle containing the DAG file. It is not present in the DAG file, so ask the user to specify it if it is not present in the context.
          - path: (optional) Path of the file relative to the bundle base directory. If not specified, validates the entire bundle.

        Returns:
           If a DAG in the bundle fails to parse, returns the validation errors that occurred, otherwise prints the DAG ID.
        `,
        inputSchema: { bundle: z.string(), path: z.string() }
    },
    async ({ bundle, path }) => {
        const response = await fetch("http://127.0.0.1:8092/bundles/parse", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ bundle, path }),
        })
        const json = await response.json()
        if (!response.ok) {
            return {
                isError: true,
                content: [{
                    type: "text",
                    text: String(`API error: ${response.statusText} (${response.status}): ${JSON.stringify(json["detail"])}`),
                }]
            };
        }
        let resp = []
        for (const [path, error] of Object.entries(json.import_errors)) {
            resp.push(`<result>File ${path}: Validation error: ${error}</result>`)
        }
        for (const dag_id of Object.keys(json.dags)) {
            resp.push(`<result>Dag '${dag_id}' parsed successfully</result>`)
        }
        return {
                content: [{
                    type: "text",
                    text: String(resp.join("\n")),
                }]
        };
    }
);

server.registerTool("upload_bundle",
    {
        title: "Uploads an Airflow DAG",
        description: `Use this tool to upload the Airflow DAG. 
        
        Arguments:
          - bundle: Name of the bundle containing the DAG file. It is not present in the DAG file, so ask the user to specify it if it is not present in the context.
          - path: (optional) Path of the file relative to the bundle base directory. If not specified, validates the entire bundle.

        Returns:
           If a DAG in the bundle fails to parse, returns the validation errors that occurred, otherwise prints the DAG IDs.
        `,
        inputSchema: { bundle: z.string(), path: z.string() }
    },
    async ({ bundle, path }) => {
        const response = await fetch("http://127.0.0.1:8092/bundles/parse_update", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ bundle, path }),
        })
        const json = await response.json()
        if (!response.ok) {
            return {
                isError: true,
                content: [{
                    type: "text",
                    text: String(`API error: ${response.statusText} (${response.status}): ${JSON.stringify(json["detail"])}`),
                }]
            };
        }
        let resp = []
        for (const [path, error] of Object.entries(json.import_errors)) {
            resp.push(`<result>File ${path}: Validation error: ${error}</result>`)
        }
        for (const dag_id of json.dag_ids) {
            resp.push(`<result>Dag '${dag_id}' uploaded successfully</result>`)
        }
        return {
                content: [{
                    type: "text",
                    text: String(resp.join("\n")),
                }]
        };
    }
);

// Start receiving messages on stdin and sending messages on stdout
const transport = new StdioServerTransport();
await server.connect(transport);