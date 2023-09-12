/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TimeTravelExpression
        extends Expression
{
    public enum TimeTravelType
    {
        TIMESTAMP,
        VERSION
    }

    private final Expression asOfExpr;
    private final TimeTravelType type;

    public TimeTravelExpression(TimeTravelType type, Expression value)
    {
        this(Optional.empty(), type, value);
    }

    public TimeTravelExpression(NodeLocation location, TimeTravelType type, Expression value)
    {
        this(Optional.of(location), type, value);
    }

    private TimeTravelExpression(Optional<NodeLocation> location, TimeTravelType type, Expression value)
    {
        super(location);
        requireNonNull(value, "value is null");
        requireNonNull(type, "type is null");

        this.asOfExpr = value;
        this.type = type;
    }

    public static TimeTravelExpression timestampExpr(NodeLocation location, Expression value)
    {
        return new TimeTravelExpression(Optional.of(location), TimeTravelType.TIMESTAMP, value);
    }

    public static TimeTravelExpression versionExpr(NodeLocation location, Expression value)
    {
        return new TimeTravelExpression(Optional.of(location), TimeTravelType.VERSION, value);
    }

    public static TimeTravelExpression timestampExpr(Expression value)
    {
        return new TimeTravelExpression(Optional.empty(), TimeTravelType.TIMESTAMP, value);
    }

    public static TimeTravelExpression versionExpr(Expression value)
    {
        return new TimeTravelExpression(Optional.empty(), TimeTravelType.VERSION, value);
    }

    public Expression getAsOfExpr()
    {
        return asOfExpr;
    }

    public TimeTravelType getTimeTravelType()
    {
        return type;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTimeTravel(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(asOfExpr);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimeTravelExpression that = (TimeTravelExpression) o;
        return Objects.equals(asOfExpr, that.asOfExpr) &&
                (type == that.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(asOfExpr, type);
    }
}
