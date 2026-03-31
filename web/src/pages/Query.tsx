import { Info as InfoIcon, PlayArrow as PlayIcon } from "@mui/icons-material";
import {
	Alert,
	Backdrop,
	Box,
	Button,
	CircularProgress,
	FormControl,
	InputLabel,
	MenuItem,
	Paper,
	Select,
	Snackbar,
	Table,
	TableBody,
	TableCell,
	TableContainer,
	TableHead,
	TableRow,
	TextField,
	Typography,
} from "@mui/material";
import { memo, useEffect, useState } from "react";
import { useLocation } from "react-router-dom";
import { client } from "../api/client";
import type { QueryResultRow, TestRunSummary } from "../gen/swarun_pb";

const QueryResultTable = memo(
	({
		rows,
		columnNames,
	}: {
		rows: QueryResultRow[];
		columnNames: string[];
	}) => {
		const columns =
			(columnNames?.length ?? 0) > 0
				? columnNames
				: rows.length > 0 && rows[0].columns?.fields
					? Object.keys(rows[0].columns.fields)
					: [];

		if (rows.length === 0) return null;

		return (
			<TableContainer component={Paper} sx={{ maxHeight: 600 }}>
				<Table stickyHeader size="small">
					<TableHead>
						<TableRow>
							{columns.map((col) => (
								<TableCell key={col}>
									<b>{col}</b>
								</TableCell>
							))}
						</TableRow>
					</TableHead>
					<TableBody>
						{rows.map((row, i) => (
							<TableRow
								key={
									row.columns?.fields.timestamp?.kind?.case === "stringValue"
										? `${row.columns.fields.timestamp.kind.value}-${i}`
										: i
								}
								hover
							>
								{columns.map((col) => (
									<TableCell key={col}>
										{(() => {
											const field = row.columns?.fields[col];
											if (!field?.kind) return "";
											if (field.kind.case === "structValue")
												return JSON.stringify(field.kind.value);
											if (field.kind.case === "listValue")
												return JSON.stringify(field.kind.value);
											return String(field.kind.value ?? "");
										})()}
									</TableCell>
								))}
							</TableRow>
						))}
					</TableBody>
				</Table>
			</TableContainer>
		);
	},
);

const QueryPage = () => {
	const location = useLocation();
	const [testRuns, setTestRuns] = useState<TestRunSummary[]>([]);
	const [selectedId, setSelectedId] = useState("");
	const [query, setQuery] = useState("SELECT * FROM metrics LIMIT 100");
	const [rows, setRows] = useState<QueryResultRow[]>([]);
	const [columnNames, setColumnNames] = useState<string[]>([]);
	const [loading, setLoading] = useState(false);
	const [error, setError] = useState<string | null>(null);

	useEffect(() => {
		const searchParams = new URLSearchParams(location.search);
		const testRunIdFromUrl = searchParams.get("testRunId");

		client.listTestRuns({}).then((resp) => {
			setTestRuns(resp.testRuns);
			if (testRunIdFromUrl) {
				setSelectedId(testRunIdFromUrl);
			} else if (resp.testRuns.length > 0) {
				setSelectedId(resp.testRuns[0].testRunId);
			}
		});
	}, [location.search]);

	const handleRunQuery = async () => {
		setLoading(true);
		setError(null);
		try {
			const response = await client.queryMetrics({
				testRunId: selectedId,
				query: query,
			});
			// フロントエンドでの表示制限 (バックエンドでも制限しているが念のため)
			setRows(response.rows.slice(0, 1000));
			setColumnNames(response.columnNames || []);
		} catch (err: unknown) {
			if (err instanceof Error) {
				setError(err.message);
			} else {
				setError("Query execution failed");
			}
			setRows([]);
			setColumnNames([]);
		} finally {
			setLoading(false);
		}
	};

	return (
		<Box sx={{ p: 3 }}>
			<Typography variant="h4" gutterBottom>
				Metrics Query
			</Typography>

			<Paper sx={{ p: 2, mb: 3 }}>
				<Box sx={{ display: "flex", gap: 2, mb: 2 }}>
					<FormControl sx={{ minWidth: 300 }}>
						<InputLabel>Test Run ID</InputLabel>
						<Select
							value={selectedId}
							label="Test Run ID"
							onChange={(e) => setSelectedId(e.target.value)}
						>
							{testRuns.map((run) => (
								<MenuItem key={run.testRunId} value={run.testRunId}>
									{run.testRunId} ({run.startTime?.toDate().toLocaleString()})
								</MenuItem>
							))}
						</Select>
					</FormControl>
					<Button
						variant="contained"
						startIcon={<PlayIcon />}
						onClick={handleRunQuery}
						disabled={!selectedId || loading}
					>
						Run Query
					</Button>
				</Box>

				<TextField
					fullWidth
					multiline
					rows={6}
					variant="outlined"
					label="Query (SQL or Flux)"
					value={query}
					onChange={(e) => setQuery(e.target.value)}
					sx={{ mb: 1, fontFamily: "monospace" }}
				/>

				<Box
					sx={{
						display: "flex",
						alignItems: "center",
						gap: 1,
						color: "text.secondary",
					}}
				>
					<InfoIcon fontSize="small" />
					<Typography variant="caption">
						Standard Table: <code>metrics</code> (timestamp, metric, value,
						path, worker_id, request_id, labels)
					</Typography>
				</Box>
			</Paper>

			<QueryResultTable rows={rows} columnNames={columnNames} />

			<Backdrop
				open={loading}
				sx={{ zIndex: (theme) => theme.zIndex.drawer + 1, color: "#fff" }}
			>
				<CircularProgress color="inherit" />
			</Backdrop>

			<Snackbar
				open={!!error}
				autoHideDuration={6000}
				onClose={() => setError(null)}
			>
				<Alert
					severity="error"
					onClose={() => setError(null)}
					sx={{ width: "100%" }}
				>
					{error}
				</Alert>
			</Snackbar>
		</Box>
	);
};

export default QueryPage;
