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
import clojure.lang.Associative;
import clojure.lang.Compiler;
import clojure.lang.Keyword;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;

public class ClojureCascadingBase extends BaseOperation {
  private byte[] serialized_spec;
  private byte[] options_spec;
  private Object[] fn_spec;

  private FlowProcess flow_process;
  private OperationCall op_call;

  private IFn fn;
  private Associative options;

  public void initialize(Object[] fn_spec, Associative options) {
    serialized_spec = KryoService.serialize(fn_spec);
    options_spec    = KryoService.serialize(options);
  }

  public ClojureCascadingBase(Object[] fn_spec, Associative options) {
    initialize(fn_spec, options);
  }

  public ClojureCascadingBase(Fields fields, Object[] fn_spec, Associative options) {
    super(fields);
    initialize(fn_spec, options);
  }

  @Override public void prepare(FlowProcess flow_process, OperationCall op_call) {
    this.fn_spec = (Object[]) KryoService.deserialize(serialized_spec);
    this.options = (Associative) KryoService.deserialize(options_spec);
    this.fn = Util.bootFn(fn_spec);

    // Bind the current flow process and opcall to dynamic variables in Cascalog's namespace.
    // These are visible as cascalog.api/*flow-process* and cascalog.api/*operation-call*.
    // They're bound twice so that they're visible to before-hooks, and visible to workers.
    if (flow_process == null)
      throw new RuntimeException("flow process is null, god help us");

    RT.var("cascalog.api", "*flow-process*").bindRoot(this.flow_process = flow_process);
    RT.var("cascalog.api", "*operation-call*").bindRoot(this.op_call = op_call);

    // Evaluate the before-hook if we have one.
    final Keyword before_hook_key = Keyword.intern("before-hook");

    if (options != null && options.containsKey(before_hook_key)) {
      final Object before_hook = options.entryAt(before_hook_key).val();
      if (before_hook != null)
        Compiler.eval(before_hook);
    }
  }

  @Override public void cleanup(FlowProcess flow_process, OperationCall op_call) {
    final Keyword after_hook_key = Keyword.intern("after-hook");
    if (options != null && options.containsKey(after_hook_key)) {
      final Object after_hook = options.entryAt(after_hook_key).val();
      if (after_hook != null)
        Compiler.eval(after_hook);
    }
  }

  protected Object applyFunction(ISeq seq) {
    try {
      // Bind the vars again, just in case someone managed to reset them.
      RT.var("cascalog.api", "*flow-process*").bindRoot(flow_process);
      RT.var("cascalog.api", "*operation-call*").bindRoot(op_call);

      return this.fn.applyTo(seq);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected Object invokeFunction(Object arg) {
    try {
      // Bind the vars again, just in case someone managed to reset them.
      RT.var("cascalog.api", "*flow-process*").bindRoot(flow_process);
      RT.var("cascalog.api", "*operation-call*").bindRoot(op_call);

      return this.fn.invoke(arg);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected Object invokeFunction() {
    try {
      // Bind the vars again, just in case someone managed to reset them.
      RT.var("cascalog.api", "*flow-process*").bindRoot(flow_process);
      RT.var("cascalog.api", "*operation-call*").bindRoot(op_call);

      return this.fn.invoke();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
