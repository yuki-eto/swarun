import { Refresh as RefreshIcon } from "@mui/icons-material";
import {
	Box,
	Chip,
	CircularProgress,
	IconButton,
	Paper,
	Table,
	TableBody,
	TableCell,
	TableContainer,
	TableHead,
	TableRow,
	TableSortLabel,
	Typography,
} from "@mui/material";
import { useCallback, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { client } from "../api/client";
import type { TestRunSummary } from "../gen/swarun_pb";

const Results = () => {
	const [testRuns, setTestRuns] = useState<TestRunSummary[]>([]);
	const [loading, setLoading] = useState(true);
	const [order, setOrder] = useState<"asc" | "desc">("desc");
	const [orderBy, setOrderBy] = useState<keyof TestRunSummary>("startTime");
	const navigate = useNavigate();

	const handleRequestSort = (property: keyof TestRunSummary) => {
		const isAsc = orderBy === property && order === "asc";
		setOrder(isAsc ? "desc" : "asc");
		setOrderBy(property);
	};

	const sortedTestRuns = [...testRuns].sort((a, b) => {
		const aValue = a[orderBy];
		const bValue = b[orderBy];

		if (orderBy === "startTime") {
			const aTime = a.startTime?.toDate().getTime() || 0;
			const bTime = b.startTime?.toDate().getTime() || 0;
			return order === "desc" ? bTime - aTime : aTime - bTime;
		}

		if (typeof aValue === "number" && typeof bValue === "number") {
			return order === "desc" ? bValue - aValue : aValue - bValue;
		}

		if (typeof aValue === "string" && typeof bValue === "string") {
			return order === "desc"
				? bValue.localeCompare(aValue)
				: aValue.localeCompare(bValue);
		}

		if (typeof aValue === "boolean" && typeof bValue === "boolean") {
			return order === "desc"
				? (bValue ? 1 : 0) - (aValue ? 1 : 0)
				: (aValue ? 1 : 0) - (bValue ? 1 : 0);
		}

		return 0;
	});

	const fetchData = useCallback(async () => {
		setLoading(true);
		try {
			const resp = await client.listTestRuns({});
			setTestRuns(resp.testRuns);
		} catch (err) {
			console.error("Failed to fetch test runs:", err);
		} finally {
			setLoading(false);
		}
	}, []);

	useEffect(() => {
		fetchData();
	}, [fetchData]);

	return (
		<Box>
			<Box
				sx={{
					display: "flex",
					justifyContent: "space-between",
					alignItems: "center",
					mb: 3,
				}}
			>
				<Typography variant="h4">Test Results</Typography>
				<IconButton onClick={fetchData} disabled={loading} color="primary">
					{loading ? <CircularProgress size={24} /> : <RefreshIcon />}
				</IconButton>
			</Box>

			<TableContainer component={Paper}>
				<Table>
					<TableHead>
						<TableRow>
							<TableCell
								sortDirection={orderBy === "testRunId" ? order : false}
							>
								<TableSortLabel
									active={orderBy === "testRunId"}
									direction={orderBy === "testRunId" ? order : "asc"}
									onClick={() => handleRequestSort("testRunId")}
								>
									Test Run ID
								</TableSortLabel>
							</TableCell>
							<TableCell
								sortDirection={orderBy === "startTime" ? order : false}
							>
								<TableSortLabel
									active={orderBy === "startTime"}
									direction={orderBy === "startTime" ? order : "asc"}
									onClick={() => handleRequestSort("startTime")}
								>
									Start Time
								</TableSortLabel>
							</TableCell>
							<TableCell
								sortDirection={orderBy === "isRunning" ? order : false}
							>
								<TableSortLabel
									active={orderBy === "isRunning"}
									direction={orderBy === "isRunning" ? order : "asc"}
									onClick={() => handleRequestSort("isRunning")}
								>
									Status
								</TableSortLabel>
							</TableCell>
							<TableCell
								align="right"
								sortDirection={orderBy === "concurrency" ? order : false}
							>
								<TableSortLabel
									active={orderBy === "concurrency"}
									direction={orderBy === "concurrency" ? order : "asc"}
									onClick={() => handleRequestSort("concurrency")}
								>
									Concurrency
								</TableSortLabel>
							</TableCell>
							<TableCell
								align="right"
								sortDirection={orderBy === "workerCount" ? order : false}
							>
								<TableSortLabel
									active={orderBy === "workerCount"}
									direction={orderBy === "workerCount" ? order : "asc"}
									onClick={() => handleRequestSort("workerCount")}
								>
									Workers
								</TableSortLabel>
							</TableCell>
							<TableCell
								align="right"
								sortDirection={orderBy === "rps" ? order : false}
							>
								<TableSortLabel
									active={orderBy === "rps"}
									direction={orderBy === "rps" ? order : "asc"}
									onClick={() => handleRequestSort("rps")}
								>
									RPS
								</TableSortLabel>
							</TableCell>
							<TableCell
								align="right"
								sortDirection={orderBy === "avgLatencyMs" ? order : false}
							>
								<TableSortLabel
									active={orderBy === "avgLatencyMs"}
									direction={orderBy === "avgLatencyMs" ? order : "asc"}
									onClick={() => handleRequestSort("avgLatencyMs")}
								>
									Avg Latency
								</TableSortLabel>
							</TableCell>
						</TableRow>
					</TableHead>
					<TableBody>
						{sortedTestRuns.map((run) => (
							<TableRow
								key={run.testRunId}
								hover
								onClick={() => navigate(`/runs/${run.testRunId}`)}
								sx={{ cursor: "pointer" }}
							>
								<TableCell>{run.testRunId}</TableCell>
								<TableCell>
									{run.startTime?.toDate().toLocaleString()}
								</TableCell>
								<TableCell>
									<Chip
										label={run.isRunning ? "Running" : "Finished"}
										color={run.isRunning ? "primary" : "default"}
										size="small"
									/>
								</TableCell>
								<TableCell align="right">{run.concurrency}</TableCell>
								<TableCell align="right">{run.workerCount}</TableCell>
								<TableCell align="right">{run.rps.toFixed(2)}</TableCell>
								<TableCell align="right">
									{run.avgLatencyMs.toFixed(2)} ms
								</TableCell>
							</TableRow>
						))}
						{!loading && testRuns.length === 0 && (
							<TableRow>
								<TableCell colSpan={7} align="center">
									No test runs found
								</TableCell>
							</TableRow>
						)}
					</TableBody>
				</Table>
			</TableContainer>
		</Box>
	);
};

export default Results;
