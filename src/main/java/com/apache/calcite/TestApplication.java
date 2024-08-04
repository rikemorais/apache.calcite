package com.apache.calcite;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class TestApplication {

  private static final List<Object[]> BOOK_DATA = Arrays.asList(
      new Object[]{1, "Les Miserables", 1862, 0},
      new Object[]{2, "The Hunchback of Notre-Dame", 1829, 0},
      new Object[]{3, "The Last Day of a Condemned Man", 1829, 0},
      new Object[]{4, "The three Musketeers", 1844, 1},
      new Object[]{5, "The Count of Monte Cristo", 1884, 1}
  );

  private static final List<Object[]> AUTHOR_DATA = Arrays.asList(
      new Object[]{0, "Victor", "Hugo"},
      new Object[]{1, "Alexandre", "Dumas"}
  );

  @Test
  public void testApp() throws Exception {

    // Instancia um Factory para os Tipos de Dados (VARCHAR, NUMERIC, etc...)
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    // Cria o Esquema Raiz que Descreve o Modelo de Dados
    CalciteSchema schema = CalciteSchema.createRootSchema(true);

    // Define os Tipos de Dados para a Tabela authors
    RelDataTypeFactory.Builder authorType = new RelDataTypeFactory.Builder(typeFactory);
      authorType.add("id", SqlTypeName.INTEGER);
      authorType.add("firstname", SqlTypeName.VARCHAR);
      authorType.add("lastname", SqlTypeName.VARCHAR);

    // Inicializa a Tabela authors com os Dados
    ListTable authorsTable = new ListTable(authorType.build(), AUTHOR_DATA);

    // Adiciona a Tabela authors para o Schema
    schema.add("author", authorsTable);

    // Define os Tipos de Dados para a Tabela books
    RelDataTypeFactory.Builder bookType = new RelDataTypeFactory.Builder(typeFactory);
      bookType.add("id", SqlTypeName.INTEGER);
      bookType.add("title", SqlTypeName.VARCHAR);
      bookType.add("year", SqlTypeName.INTEGER);
      bookType.add("author", SqlTypeName.INTEGER);

    // Inicializa a Tabela books com os Dados
    ListTable booksTable = new ListTable(bookType.build(), BOOK_DATA);

    // Adiciona a Tabela books para o Schema
    schema.add("books", booksTable);

    // Cria um SQL Parser
    SqlParser parser = SqlParser.create(
        "SELECT b.id, b.title, b.\"year\", a.firstname || ' ' || a.lastname \n"
            + "FROM Books b\n"
            + "LEFT OUTER JOIN Author a ON b.author=a.id\n"
            + "WHERE b.\"year\" > 1830\n"
            + "ORDER BY b.id\n"
            + "LIMIT 5");

    // Analisa a Consulta em um AST
    SqlNode sqlNode = parser.parseQuery();

    // Configura e Instancia o Validador
    Properties props = new Properties();
      props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
    CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,
        Collections.singletonList(""),
        typeFactory, config);

    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
        catalogReader, typeFactory,
        SqlValidator.Config.DEFAULT);

    // Valida AST Inicial
    SqlNode validNode = validator.validate(sqlNode);

    // Configura e Instancia o Conversor do Plano AST para Lógico (Requer opt cluster)
    RelOptCluster cluster = newCluster(typeFactory);
    SqlToRelConverter relConverter = new SqlToRelConverter(
        NOOP_EXPANDER,
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.config());

    // Converte o AST Válido em um Plano Lógico
    RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;

    // Mostra o Plano Lógico
    System.out.println(
        RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.EXPPLAN_ATTRIBUTES));

    // Inicializa o Otimizador e Planejador com as Regras Necessárias
    RelOptPlanner planner = cluster.getPlanner();
      planner.addRule(CoreRules.FILTER_INTO_JOIN);
      planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
      planner.addRule(Bindables.BINDABLE_FILTER_RULE);
      planner.addRule(Bindables.BINDABLE_JOIN_RULE);
      planner.addRule(Bindables.BINDABLE_PROJECT_RULE);
      planner.addRule(Bindables.BINDABLE_SORT_RULE);

    // Define o Tipo de Plano de Saída (Neste Caso Queremos um Plano Físico em BindConvention)
    logPlan = planner.changeTraits(logPlan,
        cluster.traitSet().replace(BindableConvention.INSTANCE));
    planner.setRoot(logPlan);

    // Inicia o Processo de Otimização para Obter o Plano Físico Mais Eficiente com Base no Conjunto de Regras Fornecido
    BindableRel phyPlan = (BindableRel) planner.findBestExp();

    // Mostra o Plano Físico
    System.out.println(
        RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.NON_COST_ATTRIBUTES));

    // Executa o Plano Executável Usando um Contexto que Fornece Acesso ao Esquema
    for (Object[] row : phyPlan.bind(new SchemaOnlyDataContext(schema))) {
      System.out.println(Arrays.toString(row));
    }
  }

  // Uma Tabela Baseada Numa Lista
  private static class ListTable extends AbstractTable implements ScannableTable {
    private final RelDataType rowType;
    private final List<Object[]> data;

    ListTable(RelDataType rowType, List<Object[]> data) {
      this.rowType = rowType;
      this.data = data;
    }

    @Override public Enumerable<Object[]> scan(final DataContext root) {
      return Linq4j.asEnumerable(data);
    }

    @Override public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
      return rowType;
    }
  }

  private static RelOptCluster newCluster(RelDataTypeFactory factory) {
    RelOptPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return RelOptCluster.create(planner, new RexBuilder(factory));
  }

  private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath
      , viewPath) -> null;

// Contexto de Dados com Informações do Schema
  private static final class SchemaOnlyDataContext implements DataContext {
    private final SchemaPlus schema;

    SchemaOnlyDataContext(CalciteSchema calciteSchema) {
      this.schema = calciteSchema.plus();
    }

    @Override public SchemaPlus getRootSchema() {
      return schema;
    }

    @Override public JavaTypeFactory getTypeFactory() {
      return new JavaTypeFactoryImpl();
    }

    @Override public QueryProvider getQueryProvider() {
      return null;
    }

    @Override public Object get(final String name) {
      return null;
    }
  }
}
