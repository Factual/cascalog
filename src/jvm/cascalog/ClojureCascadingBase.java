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

public class ClojureCascadingBase extends BaseOperation {
    private byte[] serialized_spec;
    private Object[] fn_spec;

    private boolean include_context;
    private FlowProcess flow_process;
    private OperationCall operation_call;
    private IFn fn;

    public void initialize(Object[] fn_spec, boolean include_context) {
        serialized_spec = KryoService.serialize(fn_spec);
        this.include_context = include_context;
    }

    public ClojureCascadingBase(Object[] fn_spec, boolean include_context) {
        initialize(fn_spec, include_context);
    }

    public ClojureCascadingBase(Fields fields, Object[] fn_spec, boolean include_context) {
        super(fields);
        initialize(fn_spec, include_context);
    }

    @Override
    public void prepare(FlowProcess flow_process, OperationCall op_call) {
        this.fn_spec = (Object[]) KryoService.deserialize(serialized_spec);
        this.fn = Util.bootFn(fn_spec);
        if (include_context) {
            this.flow_process = flow_process;
            this.operation_call = op_call;
        }
    }

    protected Object applyFunction(ISeq seq) {
        try {
            if (include_context) {
                return this.fn.applyTo(seq.cons(operation_call).cons(flow_process));
            } else {
                return this.fn.applyTo(seq);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Object invokeFunction(Object arg) {
        try {
            if (include_context) {
                return this.fn.invoke(flow_process, operation_call, arg);
            } else {
                return this.fn.invoke(arg);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    protected Object invokeFunction() {
        try {
            if (include_context) {
                return this.fn.invoke(flow_process, operation_call);
            } else {
                return this.fn.invoke();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
