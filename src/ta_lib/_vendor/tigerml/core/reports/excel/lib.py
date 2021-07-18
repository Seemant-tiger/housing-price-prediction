from tigerml.core.utils import time_now_readable

from .Report import ExcelComponentGroup, ExcelDashboard, ExcelReport


def create_excel_report(contents, columns=2, name="", path="", split_sheets=False):
    if not name:
        name = "report_at_{}".format(time_now_readable())
    report = ExcelReport(name, file_path=path)
    if split_sheets:
        for content in contents:
            if isinstance(contents, dict):
                content_name = content
                content = contents[content_name]
            else:
                content_name = "Sheet1"
            report.append_dashboard(
                create_excel_dashboard(content, name=content_name, columns=columns)
            )
    else:
        report.append_dashboard(
            create_excel_dashboard(contents, name="Sheet1", columns=columns)
        )
    report.save()


def create_excel_dashboard(contents, name="", columns=2, flatten=False):
    dash = ExcelDashboard(name=name)
    cg = create_component_group(contents, dash, columns=columns, flatten=flatten)
    dash.append(cg)
    return dash


def group_components(components, dashboard, name="", columns=2, flatten=False):
    cg = ExcelComponentGroup(dashboard, name=name, columns=columns)
    temp_cg = cg
    for component in components:
        if isinstance(component, tuple):
            # import pdb
            # pdb.set_trace()
            import copy

            old_cg = copy.deepcopy(cg)
            old_cg.name = ""
            cg = ExcelComponentGroup(dashboard, name=name, columns=1)
            cg.append(old_cg)
            current_cg = group_components(
                component[1], dashboard, component[0], columns=columns, flatten=flatten
            )
            cg.append(current_cg)
            temp_cg = ExcelComponentGroup(dashboard, name="", columns=2)
        else:
            temp_cg.append(component)
    if cg != temp_cg:
        cg.append(temp_cg)
    return cg


def create_component_group(contents, dashboard, name="", columns=2, flatten=False):
    from ..helpers import create_components

    components = create_components(contents, flatten=flatten, format="xlsx")
    cg = group_components(
        components, dashboard, name=name, columns=columns, flatten=flatten
    )
    return cg


# def create_components(contents, flatten=False):
# 	components = []
# 	for content in contents:
# 		if isinstance(contents, dict):
# 			content_name = content
# 			content = contents[content_name]
# 		else:
# 			content_name = None
# 		if isinstance(content, str):
# 			component = ExcelText(content, name=content_name)
# 		elif str(content.__class__.__module__).startswith('tigerml.core.reports.contents'):
# 			component = get_component_in_format(content, format='xlsx')
# 		elif isinstance(content, ExcelComponentGroup)
# 	    	or isinstance(content, ExcelComponent):
# 			component = content
# 		elif isinstance(content, pd.DataFrame) or isinstance(content, Styler):
# 			component = ExcelTable(content, title=content_name)
# 		elif type(content).__module__.startswith('holoviews')
# 	    	or type(content).__module__.startswith('hvplot') or \
# 			type(content).__module__.startswith('bokeh')
# 	 		or type(content).__module__.startswith('plotly'):
# 			component = ExcelImage(content, name=content_name)
# 		elif isinstance(content, Iterable):
# 			if flatten:
# 				component = create_components(content, flatten=True)
# 			else:
# 				component = (content_name, create_components(content, flatten=False))
# 		else:
# 			component = ExcelImage(content, name=content_name)
# 		# if isinstance(component, list):
# 		# 	components += component
# 		# else:
# 		components.append(component)
# 		if flatten:
# 			from tigerml.core.utils import flatten_list
# 			components = flatten_list(components)
# 	return components
