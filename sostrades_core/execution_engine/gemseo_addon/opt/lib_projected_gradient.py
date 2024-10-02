'''
Copyright 2023 Capgemini

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
import logging
from builtins import super, zip
from dataclasses import dataclass

from future import standard_library
from gemseo.algos.design_space_utils import get_value_and_bounds
from gemseo.algos.opt.base_optimization_library import (
    BaseOptimizationLibrary,
    OptimizationAlgorithmDescription,
)
from gemseo.algos.optimization_problem import OptimizationProblem
from gemseo.algos.optimization_result import OptimizationResult
from numpy import float64, isfinite, real
from scipy.optimize import fmin_tnc

"""
Projected Gradient library
"""

standard_library.install_aliases()

LOGGER = logging.getLogger("gemseo.addons.opt.lib_projected_gradient")


@dataclass
class ProjectedGradientAlgorithmDescription(OptimizationAlgorithmDescription):
    """The description of an optimization algorithm from the ProjectedGradient library."""

    library_name: str = "ProjectedGradient"


class ProjectedGradientOpt(BaseOptimizationLibrary):
    """Projected Gradient optimization library interface.

    See BaseOptimizationLibrary.
    """

    LIB_COMPUTE_GRAD = True

    _OPTIONS_MAP = {BaseOptimizationLibrary._MAX_ITER: "max_iter",
                    BaseOptimizationLibrary._F_TOL_REL: "ftol_rel",
                    BaseOptimizationLibrary._MAX_FUN_EVAL: "maxfun"
                   }
    LIBRARY_NAME = "ProjectedGradient"

    def __init__(self, algo_name):
        '''
        Constructor

        Generate the library dict, contains the list
        of algorithms with their characteristics:

        - does it require gradient
        - does it handle equality constraints
        - does it handle inequality constraints

        '''
        super(ProjectedGradientOpt, self).__init__(algo_name)
        doc = 'https://docs.scipy.org/doc/scipy/reference/'
        self.descriptions = {
            "ProjectedGradient": ProjectedGradientAlgorithmDescription(
                algorithm_name="ProjectedGradient",
                description="Projected conjugated Gradient algorithm implementation",
                require_gradient=True,
                positive_constraints=False,
                handle_equality_constraints=False,
                handle_inequality_constraints=False,
                internal_algorithm_name="ProjectedGradient",
                website=f"{doc}generated/scipy.optimize.fmin_tnc.html",
            )}

    def _get_options(self,
                     maxfun=999,
                     ftol=1e-10,  # pylint: disable=W0221
                     **kwargs):
        r"""Sets the options default values

        To get the best and up to date information about algorithms options,
        go to scipy.optimize documentation:
        https://docs.scipy.org/doc/scipy/reference/tutorial/optimize.html

        :param max_iter: maximum number of iterations, ie unique calls to f(x)
        :type max_iter: int
        :param ftol_rel: stop criteria, relative tolerance on the
               objective function,
               if abs(f(xk)-f(xk+1))/abs(f(xk))<= ftol_rel: stop
               (Default value = 1e-9)
        :param max_fun_eval: internal stop criteria on the
               number of algorithm outer iterations (Default value = 999)
        :param kwargs: other algorithms options
        :tupe kwargs: kwargs
        """
        popts = self._process_options(maxfun=kwargs['max_iter'],
                                      ftol=ftol,
                                      **kwargs)
        return popts

    def _process_options(
            self, **options  # type:Any
    ):  # type: (...) -> Dict[str, Any]
        options = BaseOptimizationLibrary._process_options(self, **options)
        self._OPTIONS_MAP[self._MAX_ITER] = options['maxfun']
        self._OPTIONS_MAP[self._NORMALIZE_DESIGN_SPACE_OPTION] = True
        options.update(self._OPTIONS_MAP)
        return options

    def _run(self, problem: OptimizationProblem, **options):
        """Runs the algorithm,

        :param options: the options dict for the algorithm
        """
        normalize_ds = options.get(self._NORMALIZE_DESIGN_SPACE_OPTION, True)
        # Get the normalized bounds:
        x_0, l_b, u_b = get_value_and_bounds(problem.design_space, normalize_ds)
        # Replace infinite values with None:
        l_b = [val if isfinite(val) else None for val in l_b]
        u_b = [val if isfinite(val) else None for val in u_b]
        bounds = list(zip(l_b, u_b))

        def real_part_fun(
                x,  # type: ndarray
        ):  # type: (...) -> Union[int, float]
            """Wrap the function and return the real part.

            Args:
                x: The values to be given to the function.

            Returns:
                The real part of the evaluation of the objective function.
            """
            return real(self.problem.objective.func.evaluate(x))

        def real_part_fun_grad(
                x,  # type: ndarray
        ):  # type: (...) -> Union[int, float]
            """Wrap the function and return the real part.

            Args:
                x: The values to be given to the function.

            Returns:
                The real part of the evaluation of the objective function.
            """
            return self.problem.objective.jac(x).real.astype(float64)

        options.pop(self.NORMALIZE_DESIGN_SPACE_OPTION)
        maxfun = options[self.MAX_ITER]
        options.pop(self.MAX_ITER)
        options.pop("maxfun")

        options.pop("max_fun_eval")
        # execute the optimization
        self._ftol_rel = 1e-10
        options.pop("ftol_rel")

        x_star, nfev, status = fmin_tnc(func=real_part_fun,
                                        x0=x_0,
                                        fprime=real_part_fun_grad,
                                        maxfun=maxfun,
                                        bounds=bounds,
                                        **options)

        x_opt = self.problem.design_space.project_into_bounds(x_star)
        f_opt = real_part_fun(x_opt)
        is_feasible = self.problem.is_point_feasible(x_opt)

        optim_result = OptimizationResult(
            x_0=x_0,
            x_opt=x_opt,
            f_opt=f_opt,
            status=status,
            constraint_values=None,
            constraints_grad=None,
            optimizer_name=self.algo_name,
            message="",
            n_obj_call=nfev,
            n_grad_call=None,
            n_constr_call=None,
            is_feasible=is_feasible,
        )
        return optim_result
