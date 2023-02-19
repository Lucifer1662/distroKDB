import { HomeContent as StoryComponent } from "../routes/index";
import type { Meta, StoryFn } from "@storybook/html";
import { ComponentProps, createSignal } from "solid-js";



function delay(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function Parent(args: any) {
    const [data, setData] = createSignal(args.initial_data)

    return <div>
        <StoryComponent
            {...args}
            get_all_local={async (id: number) => {
                await delay(args.loading_time)
                return args.nodes_data[id]
            }}
            get_node_ids={() => args.nodes.map((node: any) => node.id)}
            get_all_node_information={(ids: number[]) => Object.keys(args.nodes).map((key) => args.nodes[key]).filter(node => ids.includes(node.id))}
            set_value={async ({ key, value }) => {
                console.log("here")
                await delay(args.loading_time)
                if (args.should_succeed) {
                    setData((prev: any) => ({ ...prev, [key]: value }));
                }
                return args.should_succeed;
            }}
            get_value={async (key: number) => {
                await delay(args.loading_time)
                return data()[key]
            }}
        />
        {JSON.stringify(data())}
    </div>
}


const Template = ((args: any) => <Parent {...args} />) as StoryFn<ComponentProps<typeof StoryComponent>>;

export const HomeTemplate = Template.bind({});


HomeTemplate.args = {
    //@ts-ignore
    loading_time: 1000,

    initial_data: {
        "foo": "foo", "bar": "haha"
    },

    should_succeed: true,

    //@ts-ignore
    nodes_data: {
        0: {
            permanent_values: {
                "foo": "foo", "bar": "haha"
            },
            temporary_values: { "foo": "foo", "bar": "haha" }
        },
        1: {
            permanent_values: {
                "foo": "foo",
            },
            temporary_values: { "bar": "haha" }
        }
    },


    nodes: [
        {
            id: 0, isActive: true, position: 12304023401234
        }, {
            id: 1, isActive: false, position: 74023490242344
        }
    ]
};




export default {
    /* 👇 The title prop is optional.
     * See https://storybook.js.org/docs/html/configure/overview#configure-story-loading
     * to learn how to generate automatic titles
     */
    title: "Home Page",
    tags: ["autodocs"],
    argTypes: {
        loading_time: { control: "number", description: "The nodes" },
        nodes_data: { control: "object", description: "The nodes" },
        nodes: { control: "object", description: "The nodes" },
        should_succeed: { control: "bool", description: "The nodes" },
    }
} as Meta<ComponentProps<typeof StoryComponent>>;