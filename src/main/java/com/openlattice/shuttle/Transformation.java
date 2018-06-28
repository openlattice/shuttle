package com.openlattice.shuttle;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.function.Function;

@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include= JsonTypeInfo.As.PROPERTY, property="@transform")
public abstract class Transformation<I extends Object, O extends Object> implements Function<I,O> {

}