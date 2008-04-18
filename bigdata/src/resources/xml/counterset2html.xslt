<?xml version="1.0" encoding="UTF-8"?>
<?xmlspysamplexml C:\Documents and Settings\bthompson\workspace\bigdata\src\resources\xml\counterset-example1.xml?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output method="html" version="4.01" encoding="UTF-8" indent="yes"/>
	<!-- Whenever you match any node or any attribute -->
	<xsl:template match="node()|@*">
		<!-- Copy the current node -->
		<xsl:copy>
			<!-- Including any attributes it has and any child nodes -->
			<xsl:apply-templates select="@*|node()"/>
		</xsl:copy>
	</xsl:template>
</xsl:stylesheet>
