import { Card } from "@suid/material";
import { For, createResource, createSignal } from "solid-js";


interface NodeInformation {
  id: number,
  isActive: boolean,
  position: number
}

interface Props {
  nodes: NodeInformation[],
  onClickNode: (node: NodeInformation, index: number) => void
}

export function Nodes(props: Props) {
  return (
    <div>
      <Card>
        <table>
          <thead>
            <tr>
              <th>Nodes</th>
            </tr>

            <tr>
              <th>Node Id</th>
              <th>Activity</th>
              <th>Position in ring</th>
            </tr>
          </thead>

          <tbody>
            <For each={props.nodes}>
              {(node, index) => <tr onClick={() => props.onClickNode(node, index())}><td>{node.id}</td><td>{node.isActive ? "Active" : "Inactive"}</td><td>{node.position}</td></tr>}
            </For>
          </tbody>
        </table>
      </Card>
    </div >
  );
};