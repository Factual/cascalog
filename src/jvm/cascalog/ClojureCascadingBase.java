/*
    Copyright 2010 Nathan Marz

    Project and contact information: http://www.cascalog.org/

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package cascalog;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;

public class ClojureCascadingBase extends BaseOperation {
    private byte[] serialized_spec;
    private Object[] fn_spec;

    private boolean include_context;
    private FlowProcess flow_process;
    private OperationCall operation_call;
    private IFn fn;

    public void initialize(Object[] fn_spec) {
        serialized_spec = KryoService.serialize(fn_spec);
    }

    public ClojureCascadingBase(Object[] fn_spec) {
        initialize(fn_spec);
    }

    public ClojureCascadingBase(Fields fields, Object[] fn_spec) {
        super(fields);
        initialize(fn_spec);
    }

    @Override
    public void prepare(FlowProcess flow_process, OperationCall op_call) {
        this.fn_spec = (Object[]) KryoService.deserialize(serialized_spec);
        this.fn = Util.bootFn(fn_spec);

        // Bind the current flow process and opcall to dynamic variables in Cascalog's namespace.
        // These are visible as cascalog.api/*flow-process* and cascalog.api/*operation-call*.
        RT.var("cascalog.api", "*flow-process*").bindRoot(flow_process);
        RT.var("cascalog.api", "*operation-call*").bindRoot(op_call);

    }

    protected Object applyFunction(ISeq seq) {
        try {
            return this.fn.applyTo(seq);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Object invokeFunction(Object arg) {
        try {
            return this.fn.invoke(arg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    protected Object invokeFunction() {
        try {
            return this.fn.invoke();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
