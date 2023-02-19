import { createSignal } from "solid-js";
import type { Component } from "solid-js";

interface Props {
  initialValue?: number;
}

export const Counter: Component<Props> = (props) => {
  const [count, setCount] = createSignal(props.initialValue ?? 0);

  return (
    <div>
      <span>{props.initialValue}</span>
      <button onClick={() => setCount((c) => c + 1)}>Click</button>{" "}
      <span>{count()}</span>
      <span>{count()}</span>
    </div>
  );
};