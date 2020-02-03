import scipy.misc
import math

if hasattr(scipy.misc, 'comb'):
    scipy_comb = scipy.misc.comb
else:
    import scipy.special
    scipy_comb = scipy.special.comb


def chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def sidak_alpha(alpha, m):
    """
    Compute new significance level alpha for M independent tests.
    More tests -> unexpected events occur more often thus alpha has to be adjusted.
    Overall test battery fails if min(pvals) < new_alpha.
    """
    return 1 - (1 - alpha)**(1./m)


def sidak_inv(alpha, m):
    """
    Inverse transformation of sidak_alpha function.
    Used to compute final p-value of M independent tests if while preserving the
    same significance level for the resulting p-value.
    """
    return 1 - (1 - alpha)**m


def merge_pvals(pvals, batch=2):
    """
    Merging pvals with Sidak.

    Note that the merging tree has to be symmetric, otherwise the computation on pvalues is not correct.
    Note: 1-(1-(1-(1-x)^3))^2 == 1-((1-x)^3)^2 == 1-(1-x)^6.
    Example: 12 nodes, binary tree: [12] -> [2,2,2,2,2,2] -> [2,2,2]. So far it is symmetric.
    The next layer of merge is problematic as we merge [2,2] and [2] to two p-values.
    If a minimum is from [2,2] (L) it is a different expression as from [2] R as the lists
    have different lengths. P-value from [2] would increase in significance level compared to Ls on this new layer
    and this it has to be corrected.
    On the other hand, the L minimum has to be corrected as well as it came from
    list of the length 3. We want to obtain 2 p-values which can be merged as if they were equal (exponent 2).
    Thus the exponent on the [2,2] and [2] layer will be 3/2 as we had 3 p-values in total and we are producing 2.
    """
    if len(pvals) <= 1:
        return pvals

    batch = min(max(2, batch), len(pvals))  # norm batch size
    parts = list(chunks(pvals, batch))
    exponent = len(pvals) / len(parts)
    npvals = []
    for p in parts:
        pi = sidak_inv(min(p), exponent)
        npvals.append(pi)
    return merge_pvals(npvals, batch)


def booltest_pval(nfails=1, ntests=36, alpha=1/20000):
    acc = [scipy_comb(ntests, k) * (1 - alpha) ** (ntests - k) * alpha ** k for k in range(nfails)]
    return max(0, 1 - sum(acc))


def booltest_pval_log(nfails=1, ntests=36, alpha=1/20000):
    log = math.log2
    acc = [2 ** (log(scipy_comb(ntests, k)) + (ntests - k) * log((1 - alpha)) + k * log(alpha)) for k in range(nfails)]
    return max(0, 1 - sum(acc))
