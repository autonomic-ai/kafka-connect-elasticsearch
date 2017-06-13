<#list dependencyMap as e>
<#assign p = e.getKey()/>
<#assign licenses = e.getValue()?join(" | ")/>
"${p.groupId}","${p.artifactId}","${p.version}","${licenses}"
</#list>
