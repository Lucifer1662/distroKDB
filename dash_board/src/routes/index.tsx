import { For, Show, createMemo, createResource, createSignal } from "solid-js";
import { Title } from "solid-start";
import { AllLocalResponse, NodeInformation, NodeOps, ObjectOps, ObjectsOps } from "../api/distrokeydatabase";
import { NodeTable } from "../components/Node_Table";
import { Nodes } from "../components/Nodes";
import { Card, CardContent } from "@suid/material";
import { GetValue } from "../components/GetValue";
import { SetValue } from "../components/SetValue";


async function get_all_node_information(node_ids?: number[]) {
  if (node_ids) {
    const promises = node_ids.map((id) => NodeOps.get_node_information(id))
    return (await Promise.all(promises)).filter((node) => node) as NodeInformation[]
  } else {
    return undefined
  }
}

interface Props {
  get_node_ids: () => Promise<number[]>
  get_all_node_information: (ids: number[]) => Promise<NodeInformation[] | undefined>
  get_all_local: (id: number) => Promise<AllLocalResponse | undefined>
  get_value: (key: string) => Promise<string | undefined>
  set_value: (params: { key: string, value: string }) => Promise<boolean>
}

export function HomeContent(props: Props) {
  const [node_ids] = createResource(props.get_node_ids);
  const [nodes] = createResource(node_ids(), props.get_all_node_information);
  const [selected_node, set_selected_node] = createSignal<NodeInformation | undefined>(undefined)
  const [selected_node_data, { mutate, refetch }] = createResource(selected_node, (node) => {
    return props.get_all_local(node.id)
  });

  return (
    <main>
      <table>
        <tbody>
          <tr>
            <td>
              <table>
                <tbody>
                  <tr>
                    <td>
                      <Show when={nodes()} fallback={<div></div>}>
                        <Nodes nodes={nodes() || []} onClickNode={(node) => {
                          if (node != selected_node()) mutate()
                          set_selected_node(node);;;
                        }} />
                      </Show>
                      <br />
                    </td>
                  </tr>
                  <tr>
                    <td>
                      <Show when={selected_node()} fallback={<Card>No node selected</Card>}>
                        <NodeTable
                          node_id={selected_node()?.id || 0}
                          primary_data={selected_node_data()?.permanent_values}
                          temporary_data={selected_node_data()?.temporary_values} />
                      </Show>
                    </td>
                  </tr>

                </tbody>
              </table>
            </td>

            <td style={{ "vertical-align": "top" }}>
              <Card>
                <CardContent>
                  <GetValue getValue={props.get_value} />
                  <br />
                  <br />
                  <SetValue setValue={props.set_value} />
                </CardContent>
              </Card>

            </td>
          </tr>



        </tbody>
      </table>
    </main>
  );
}



export default function Home() {
  return <HomeContent
    get_all_node_information={get_all_node_information}
    get_node_ids={NodeOps.get_node_ids}
    get_all_local={ObjectsOps.get_all_local}
    get_value={ObjectOps.get}
    set_value={({ key, value }) => ObjectOps.add(key, value)}
  />
}

