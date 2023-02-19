import {
  Card, Divider, CircularProgress, Typography, Table,
  TableBody,
  TableCell,
  TableContainer,
  TableFooter,
  TableHead,
  TableRow,
  Paper,
  LinearProgress
} from "@suid/material";
import { For, Show, createResource, createSignal } from "solid-js";

interface Props {
  node_id: number,
  primary_data?: { [key: string]: string }
  temporary_data?: { [key: string]: string }
}

function TableComp(props: { title: string, data?: { [key: string]: string } }) {
  return (<div style={{ padding: "10px" }}>
    {/* <table style={{
      "border": "1px solid black",
      "border-collapse": "collapse"
    }}>

      <thead>
        <tr>
          <th colspan="2"> <Typography variant="h6">{props.title}</Typography></th>
        </tr>
        <tr>
          <th>Key</th>
          <th>Value</th>
        </tr>
      </thead>
      <tbody>
        <Show when={props.data} fallback={<CircularProgress />}>
          <For each={Object.keys(props.data || {})}>
            {key => <tr><td>{key}</td><td>{
              //@ts-ignore
              props.data[key]
            }</td></tr>}
          </For>
        </Show>

      </tbody>

    </table > */}

    <TableContainer component={Paper}>
      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell colspan="2"> <Typography variant="h6">{props.title}</Typography></TableCell>
          </TableRow>
          <TableRow>
            <TableCell>Key</TableCell>
            <TableCell>Value</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <Show when={props.data}
            fallback={
              <>
                <TableRow>
                  <TableCell><LinearProgress /></TableCell>
                  <TableCell><LinearProgress /></TableCell>
                </TableRow>
                <TableRow>
                  <TableCell><LinearProgress /></TableCell>
                  <TableCell><LinearProgress /></TableCell>
                </TableRow>

              </>
            }
          >
            <For each={Object.keys(props.data || {})}>
              {key => <TableRow>
                <TableCell>{key}</TableCell>
                <TableCell>{
                  //@ts-ignore
                  props.data[key]
                }</TableCell>
              </TableRow>}
            </For>
          </Show>


          {/* {mapArray(
            () => rows,
            (row) => (
              <TableRow
                sx={{ "&:last-child td, &:last-child th": { border: 0 } }}
              >
                <TableCell component="th" scope="row">
                  {row.name}
                </TableCell>
                <TableCell align="right">{row.calories}</TableCell>
                <TableCell align="right">{row.fat}</TableCell>
                <TableCell align="right">{row.carbs}</TableCell>
                <TableCell align="right">{row.protein}</TableCell>
              </TableRow>
            )
          )} */}
        </TableBody>
      </Table>
    </TableContainer>


  </div >);
}

export function NodeTable(props: Props) {
  return (
    <div>
      <Card>
        <Typography variant="h5"> Node: {props.node_id}</Typography>
        <TableComp title="Permanent" data={props.primary_data} />
        <br />
        <Divider />
        <br />
        <TableComp title="Temporary" data={props.temporary_data} />
      </Card>
    </div >
  );
};