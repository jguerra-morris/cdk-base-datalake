from aws_cdk import Stack

def create_name(scope: Stack, resource: str, functionality: str, case_type: str = "kebab") -> str:
    project = scope.node.try_get_context("project")
    region = scope.node.try_get_context("region")
    environment = scope.node.try_get_context("env")

    def cap_f(val: str) -> str:
        return str(val)[0].upper() + str(val)[1:] if val else ""

    if case_type == "camel":
        return (
            f"{cap_f(project)}{cap_f(region)}{cap_f(resource)}"
            f"{cap_f(environment)}{cap_f(functionality)}"
        ).replace("-", "")
    elif case_type == "nodash":
        return f"{project}{region}{resource}{environment}{functionality}"
    else:
        return f"{project}-{region}-{resource}-{environment}-{functionality}"
