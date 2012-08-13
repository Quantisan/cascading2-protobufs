package com.squareup.cascading2.function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.squareup.cascading2.util.Util;

/**
 * Created with IntelliJ IDEA. User: duxbury Date: 8/6/12 Time: 11:31 AM To change this template use
 * File | Settings | File Templates.
 */
public class ExpandProto<T extends Message> extends BaseOperation implements Function {
  private final String messageClassName;
  private final String[] fieldsToExtract;

  private transient Descriptors.FieldDescriptor[] fieldDescriptorsToExtract;

  /** Expand the entire struct, using the same field names as they are found in the struct. */
  public ExpandProto(Class<T> messageClass) {
    this(messageClass, Util.getAllFields(messageClass));
  }

  /** Expand the entire struct, using the supplied field names to name the resultant fields. */
  public ExpandProto(Class<T> messageClass, Fields fieldDeclaration) {
    this(messageClass, fieldDeclaration, Util.getAllFields(messageClass));
  }

  /**
   * Expand only the fields listed in fieldsToExtract, using the same fields names as they are found
   * in the struct.
   */
  public ExpandProto(Class<T> messageClass, String... fieldsToExtract) {
    this(messageClass, new Fields(fieldsToExtract), fieldsToExtract);
  }

  /**
   * Expand only the fields listed in fieldsToExtract, naming them with the corresponding field
   * names in fieldDeclaration.
   */
  public ExpandProto(Class<T> messageClass, Fields fieldDeclaration, String... fieldsToExtract) {
    super(1, fieldDeclaration);
    if (fieldDeclaration.size() != fieldsToExtract.length) {
      throw new IllegalArgumentException("Fields "
          + fieldDeclaration
          + " doesn't have enough field names to identify all "
          + fieldsToExtract.length
          + " fields in "
          + messageClass.getName());
    }

    Message.Builder builder = Util.builderFromMessageClass(messageClass.getName());

    for (int i = 0; i < fieldsToExtract.length; i++) {
      if (builder.getDescriptorForType().findFieldByName(fieldsToExtract[i]) == null) {
        throw new IllegalArgumentException("Could not find a field named '"
            + fieldsToExtract[i]
            + "' in message class "
            + messageClass.getName());
      }
    }

    this.fieldsToExtract = fieldsToExtract;
    this.messageClassName = messageClass.getName();
  }

  @Override public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    T arg = (T) functionCall.getArguments().getObject(0);

    Tuple result = Util.expandMessage(this.fieldDescriptorsToExtract, this.messageClassName, this.fieldsToExtract, arg);

    functionCall.getOutputCollector().add(result);
  }
}
