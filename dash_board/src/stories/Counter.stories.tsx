import { Counter } from "../components/Counter";
import type { Meta, StoryFn } from "@storybook/html";
import type { ComponentProps } from "solid-js";

const Template = ((args) => <Counter {...args} />) as StoryFn<
    ComponentProps<typeof Counter>
>;

export const CounterTemplate = Template.bind({});

CounterTemplate.args = {
    initialValue: 10,
};

// Simple examples
export const CounterDefault = () => <Counter />;

export const CounterWithProps = () => <Counter initialValue={123} />;


// example with Template


export default {
    /* 👇 The title prop is optional.
     * See https://storybook.js.org/docs/html/configure/overview#configure-story-loading
     * to learn how to generate automatic titles
     */
    title: "Counter",
    tags: ["autodocs"],
    // render: Template,
    // render: (props) => <Counter {...props} initialValue={123} />,
    argTypes: {
        initialValue: { control: "number", description: "Sets a number" },
    },
} as Meta<ComponentProps<typeof Counter>>;