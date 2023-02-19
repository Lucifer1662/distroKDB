import { NodeTable as StoryComponent } from "../components/Node_Table";
import type { Meta, StoryFn } from "@storybook/html";
import type { ComponentProps } from "solid-js";

const Template = ((args: any) => <StoryComponent {...args} />) as StoryFn<ComponentProps<typeof StoryComponent>>;
export const CounterTemplate = Template.bind({});
CounterTemplate.args = {
    node_id: 10,
    primary_data: {
        "foo": "foo", "bar": "haha"
    },
    temporary_data: { "foo": "foo", "bar": "haha" }
};


// Simple examples
export const NodeTableDefault = () => <StoryComponent node_id={0} primary_data={{}} temporary_data={{}} />;
export const NodeTableWithData = () => <StoryComponent node_id={0}
    primary_data={{ "foo": "foo", "bar": "haha" }} temporary_data={{ "foo": "foo", "bar": "haha" }} />;



export default {
    /* 👇 The title prop is optional.
     * See https://storybook.js.org/docs/html/configure/overview#configure-story-loading
     * to learn how to generate automatic titles
     */
    title: "NodeTable",
    tags: ["autodocs"],
    argTypes: {
        node_id: { control: "number", description: "Sets a node number" },
        data: { control: "object", description: "Sets data from db" },
        // node_id: 0,
        // data: { "foo": "foo", "bar": "haha" }
    }
} as Meta<ComponentProps<typeof StoryComponent>>;