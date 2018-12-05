/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
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
 * </p>
 */

package io.shardingsphere.core.parsing.antlr.extractor.segment.engine.dialect;

import java.util.HashMap;

import org.antlr.v4.runtime.ParserRuleContext;

import com.google.common.base.Optional;

import io.shardingsphere.core.parsing.antlr.extractor.segment.OptionalSQLSegmentExtractor;
import io.shardingsphere.core.parsing.antlr.extractor.segment.constant.RuleName;
import io.shardingsphere.core.parsing.antlr.extractor.segment.engine.ColumnSegmentExtractor;
import io.shardingsphere.core.parsing.antlr.extractor.segment.engine.SubqueryExtractor;
import io.shardingsphere.core.parsing.antlr.extractor.util.ASTUtils;
import io.shardingsphere.core.parsing.antlr.sql.segment.column.ColumnSegment;
import io.shardingsphere.core.parsing.antlr.sql.segment.expr.CommonExpressionSegment;
import io.shardingsphere.core.parsing.antlr.sql.segment.expr.ExpressionSegment;
import io.shardingsphere.core.parsing.antlr.sql.segment.expr.FunctionExpressionSegment;
import io.shardingsphere.core.parsing.antlr.sql.segment.expr.PropertyExpressionSegment;
import io.shardingsphere.core.parsing.antlr.sql.segment.expr.StarExpressionSegment;
import io.shardingsphere.core.parsing.antlr.sql.segment.expr.SubquerySegment;
import io.shardingsphere.core.parsing.lexer.token.Symbol;
import io.shardingsphere.core.util.SQLUtil;

/**
 * Expression extractor.
 * 
 * @author duhongjun
 */
public class ExpressionExtractor implements OptionalSQLSegmentExtractor {
    
    @Override
    public Optional<ExpressionSegment> extract(ParserRuleContext expressionNode) {
        String firstChildText = expressionNode.getText();
        if (firstChildText.endsWith(Symbol.STAR.getLiterals())) {
            int position = firstChildText.indexOf(Symbol.DOT.getLiterals());
            Optional<String> owner = Optional.absent();
            if (0 < position) {
                owner = Optional.of(SQLUtil.getExactlyValue(firstChildText.substring(0, position)));
            }
            return Optional.<ExpressionSegment>of(new StarExpressionSegment(expressionNode.getStart().getStartIndex(), owner));
        }
        Optional<ParserRuleContext> subqueryNode = ASTUtils.findFirstChildNode(expressionNode, RuleName.SUBQUERY);
        if(subqueryNode.isPresent()) {
            Optional<SubquerySegment> subquerySegment = new SubqueryExtractor().extract(subqueryNode.get());
            if(subquerySegment.isPresent()) {
                return Optional.<ExpressionSegment>of(subquerySegment.get());
            }
            return Optional.absent();
        }
        return fillForPropertyOrFunction(expressionNode);
    }
    
    private Optional<ExpressionSegment> fillForPropertyOrFunction(final ParserRuleContext node) {
        Optional<ParserRuleContext> aliasNode = ASTUtils.findFirstChildNode(node, RuleName.ALIAS);
        Optional<String> alias = null;
        if (aliasNode.isPresent()) {
            alias = Optional.of(SQLUtil.getExactlyValue(aliasNode.get().getText()));
        } else {
            alias = Optional.absent();
        }
        Optional<ParserRuleContext> functionCall = ASTUtils.findFirstChildNode(node, RuleName.FUNCTION_CALL);
        if (functionCall.isPresent()) {
            String name = functionCall.get().getChild(0).getText();
            int startIndex = functionCall.get().getStart().getStartIndex() + name.length();
            return Optional.<ExpressionSegment>of(new FunctionExpressionSegment(name, alias, startIndex, functionCall.get().getStop().getStopIndex()));
        } 
        
        if (RuleName.COLUMN_NAME.getName().equals(node.getChild(0).getClass().getSimpleName())) {
            ParserRuleContext columnNode = (ParserRuleContext)node.getChild(0);
            Optional<ColumnSegment> columnSegment = new ColumnSegmentExtractor(new HashMap<String, String>()).extract(columnNode);
            return Optional.<ExpressionSegment>of(new PropertyExpressionSegment(columnSegment.get().getOwner(), columnSegment.get().getName(),
                    columnNode.getStart().getStartIndex(), columnNode.getStop().getStopIndex(), alias));
        } 
        return Optional.<ExpressionSegment>of(new CommonExpressionSegment(node.getStart().getStartIndex(), node.getStop().getStopIndex(), alias));
    }
}
