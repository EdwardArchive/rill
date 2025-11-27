<script lang="ts">
  import * as Alert from "@rilldata/web-common/components/alert-dialog/index.js";
  import { Button } from "@rilldata/web-common/components/button";
  import AlertCircle from "svelte-radix/ExclamationTriangle.svelte";

  export let open: boolean;
  export let currentConnector: string;
  export let newConnector: string;
  export let onConfirm: () => void;
  export let onCancel: () => void;
</script>

<Alert.Root bind:open>
  <Alert.Content class="min-w-[500px]">
    <Alert.Header>
      <div class="flex items-center gap-3">
        <AlertCircle class="text-amber-500 size-5 flex-shrink-0" />
        <Alert.Title>Change default OLAP connector?</Alert.Title>
      </div>
      <Alert.Description>
        <p class="mt-3 text-slate-600">
          You are adding a new
          <code class="bg-blue-100 px-1.5 py-0.5 rounded text-blue-700"
            >{newConnector}</code
          >
          connector. Would you also like to set it as the default OLAP connector?
        </p>

        <div
          class="bg-amber-50 border border-amber-200 rounded-md p-3 mt-3 text-left"
        >
          <p class="text-amber-800 font-medium text-sm mb-1">
            Changing the default may cause errors in existing resources:
          </p>
          <ul class="text-amber-700 text-xs space-y-1 ml-4 list-disc">
            <li>Models referencing the current OLAP connector</li>
            <li>Metrics views and dashboards</li>
            <li>Canvas components with data dependencies</li>
          </ul>
        </div>

        <p class="text-slate-500 text-xs mt-3">
          Current default:
          <code class="bg-slate-100 px-1 rounded">{currentConnector || "duckdb"}</code>
          {#if !currentConnector}
            <span class="text-slate-400">(implicit)</span>
          {/if}
        </p>
      </Alert.Description>
    </Alert.Header>

    <Alert.Footer class="mt-5">
      <Button
        type="secondary"
        onClick={() => {
          open = false;
          onCancel();
        }}>No, just add connector</Button
      >
      <Button
        type="primary"
        onClick={() => {
          open = false;
          onConfirm();
        }}>Yes, set as default</Button
      >
    </Alert.Footer>
  </Alert.Content>
</Alert.Root>
