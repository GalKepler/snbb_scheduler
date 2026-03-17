# Processing Pipeline

This section describes the automated MRI processing pipeline implemented at the Strauss Neuroplasticity Brain Bank (SNBB). All processing stages were orchestrated by a custom rule-based scheduler that submitted jobs to a Slurm high-performance computing cluster, ensuring reproducibility and minimal manual intervention. The pipeline comprised six sequential stages executed in dependency order: (1) DICOM to BIDS conversion, (2) post-conversion fieldmap processing, (3) anatomical image defacing, (4) diffusion MRI preprocessing, (5) cortical surface reconstruction, and (6) diffusion MRI reconstruction and tractography. All neuroimaging tools were executed within Apptainer containers to guarantee version-locked, reproducible environments across compute nodes. Data were organized according to the Brain Imaging Data Structure (BIDS) specification throughout all stages (Gorgolewski et al., 2016).

## DICOM to BIDS Conversion

Raw DICOM files were converted to NIfTI format and organized into a BIDS-compliant directory structure using HeuDiConv 1.3.4 (Halchenko et al., 2024) with dcm2niix as the conversion backend (Li et al., 2016). A custom Python heuristic file mapped Siemens scanner protocol names to BIDS-compliant filenames and directory structures. The heuristic supported both current (2024+) and legacy protocol naming conventions used at the scanning site. Converted modalities included T1-weighted MPRAGE, T2-weighted SPC, FLAIR, multi-shell diffusion MRI (anterior–posterior and posterior–anterior phase-encoding directions), spin-echo field maps for functional distortion correction, resting-state fMRI, and multiple task fMRI paradigms. Siemens bias-field-corrected reconstructions (NORM) were stored with the BIDS `rec-norm` entity. Scanner-derived diffusion maps (ADC, FA, ColFA), localizer sequences, and IR-EPI TI series were excluded from conversion. Key HeuDiConv flags included `--converter dcm2niix`, `--bids notop` (to suppress top-level BIDS metadata regeneration on re-runs), and `--grouping all`.

## Post-BIDS Fieldmap Processing

Following DICOM conversion, a custom Python post-processing script (using nibabel and NumPy) performed three operations on each session's BIDS output. First, b=0 volumes (b < 100 s/mm²) were extracted from the posterior–anterior (PA) phase-encoded DWI acquisition, and their mean was computed to produce a three-dimensional b0 fieldmap EPI image, which was written to the `fmap/` directory with the BIDS `acq-dwi` entity alongside a copy of the original JSON sidecar. Second, `IntendedFor` fields were added to all fieldmap JSON sidecars to establish the BIDS-required linkage between field maps and their target acquisitions: DWI field maps (`acq-dwi`) were linked to anterior–posterior DWI files, and functional field maps (`acq-func`) were linked to BOLD runs. Third, spurious `.bvec` and `.bval` files in the `fmap/` directory — artifacts of the DWI-derived fieldmap creation — were hidden by prepending a dot to their filenames to prevent interference with downstream BIDS validators and processing tools.

## Anatomical Image Defacing

To protect participant privacy, all T1-weighted and T2-weighted anatomical images were defaced using `fsl_deface` from the FMRIB Software Library (FSL; Jenkinson et al., 2012). Defaced copies were written alongside the original images using the BIDS `acq-defaced` entity (e.g., `sub-XXXX_ses-YY_acq-defaced_T1w.nii.gz`), preserving the originals for processing steps that require intact facial geometry. JSON sidecars were copied alongside each defaced image.

## Diffusion MRI Preprocessing — QSIPrep

Diffusion MRI data were preprocessed using QSIPrep 1.1.1 (Cieslak et al., 2021), executed via Apptainer. Preprocessing was performed per session and included susceptibility distortion correction, eddy current correction, motion correction, and anatomical coregistration. Preprocessed diffusion data were resampled to an isotropic voxel size of 1.6 mm. The MNI152NLin2009cAsym template was used as the anatomical reference space, with a sessionwise anatomical reference strategy. A BIDS filter file restricted processing to anterior–posterior phase-encoded DWI acquisitions, with `rec-norm` T1-weighted images preferred as the anatomical input.

## Cortical Surface Reconstruction — FreeSurfer

Cortical surface reconstruction was performed using FreeSurfer 8.1.0 (Fischl, 2012), executed via Apptainer. Unlike all other pipeline stages, FreeSurfer processing was scoped at the subject level rather than the session level, with a single job processing all available sessions for a given subject.

The pipeline automatically selected between two processing strategies based on the number of available sessions. For subjects with a single session, a standard cross-sectional reconstruction was performed using `recon-all -all`. For subjects with two or more sessions, the full three-step longitudinal pipeline was executed (Reuter et al., 2012): (1) an independent cross-sectional `recon-all` for each session, (2) construction of an unbiased within-subject template via `recon-all -base` using all cross-sectional outputs as timepoints, and (3) longitudinal refinement of each session's reconstruction initialized from the template via `recon-all -long`. Already-completed steps (identified by the presence of `scripts/recon-all.done`) were automatically skipped, allowing failed jobs to resume without reprocessing.

Anatomical image selection followed a two-step filtering procedure applied per session: (1) images containing "defaced" in the filename were excluded, and (2) `rec-norm` (bias-field corrected) variants were preferred when available. When a T2-weighted image was available for a session, it was included via the `-T2` and `-T2pial` flags to improve pial surface placement.

## Diffusion MRI Reconstruction and Tractography — QSIRecon

Reconstruction was performed using QSIRecon 1.2.0 (Cieslak et al., 2021), executed via Apptainer, according to the "gal_multishell_scalars" pipeline. QSIRecon operated on the preprocessed QSIPrep outputs and required FreeSurfer cortical reconstructions for anatomically constrained tractography. A hybrid surface/volume segmentation (HSVS) was created from FreeSurfer outputs (Smith et al., 2020), and FreeSurfer outputs were registered to the QSIRecon outputs. T1w-based spatial normalization calculated during preprocessing was used to map atlases from template space into alignment with the diffusion data. Brain masks from ANTs brain extraction (Avants et al., 2008) were used in all subsequent reconstruction steps. Many internal operations of QSIRecon used Nipype 1.9.1 (Gorgolewski et al., 2011; Gorgolewski et al., 2018), Nilearn 0.10.1 (Abraham et al., 2014), and DIPY 1.10.0 (Garyfallidis et al., 2014).

### Microstructural Model Fitting

Four diffusion microstructure models were fitted in native diffusion space.

**Diffusion Kurtosis Imaging (DKI).** Diffusion kurtosis imaging (Jensen et al., 2005) was computed using DIPY (Garyfallidis et al., 2014), including white matter tract integrity (WMTI) metrics.

**Neurite Orientation Dispersion and Density Imaging (NODDI).** The NODDI model (Zhang et al., 2012) was fit using the AMICO implementation (Daducci et al., 2015). All b-values were rounded to the closest 50.0 s/mm². The isotropic diffusivity constant was set to 0.003 mm²/s. The parallel diffusivity constant was set to 0.0017 mm²/s. Diffusion-weighted images were normalized to the mean non-diffusion-weighted (b0) signal to reduce intensity variation across volumes. For each voxel, the mean signal intensity was computed across all b0 volumes identified in the acquisition scheme. Voxelwise normalization factors were then defined as the reciprocal of these mean b0 values. To prevent division by noise-dominated or near-zero intensities, voxels with mean b0 signal below a fixed threshold (defined as b0 × mean of all positive b0 values) were excluded from normalization by setting their factors to zero. The resulting normalization factors were applied to each diffusion-weighted volume by voxelwise multiplication, yielding data expressed as signal intensity relative to the mean b0 image. Peak directions were estimated from a diffusion tensor model using OLS fitting in DIPY (Garyfallidis et al., 2014). The normalized root mean square error (NRMSE) and root mean square error (RMSE) between the measured and fitted signals were computed. Intracellular volume fraction (ICVF) and orientation dispersion maps were multiplied by the tissue fraction (1 − isotropic volume fraction) in AMICO to produce tissue-fraction-modulated maps (Parker et al., 2021). As AMICO does not save the tissue fraction map, the output tissue fraction map was separately reconstructed using custom Python code matching the AMICO implementation.

**Mean Apparent Propagator MRI (MAP-MRI).** MAP-MRI reconstruction (Ozarslan et al., 2013) was performed with DIPY 1.10.0 (Garyfallidis et al., 2014). Delta information was not provided, resulting in possibly imprecise MAP-MRI reconstruction. The isotropic MAP-MRI basis was used. B-values less than 50 s/mm² were treated as b0s for the purposes of modeling. The b-value threshold for scale factor estimation was set to 2000 s/mm². The CVXPY solver was set to None. The isotropic tissue diffusivity was set to the static diffusivity constant. The eigenvalue threshold was set to 0.0001. Positivity was enforced on a grid determined by the local positivity constraint (grid size 15, adaptive radius). The propagator was not constrained to be positive. The Laplacian of the MAP-MRI basis was used for regularization with a weighting of 0.2. The radial order of the MAP-MRI model was set to 6. Static tissue diffusivity was set to 0.0007 mm²/s.

**Generalized Q-Sampling Imaging (GQI).** Diffusion orientation distribution functions (ODFs) were reconstructed using generalized q-sampling imaging (GQI; Yeh et al., 2010) with a ratio of mean diffusion distance of 1.25 in DSI Studio (version 94b9c79). Scalar maps were subsequently exported.

### Template Registration

All native-space scalar maps from the four microstructural models were warped to the MNI152NLin2009cAsym template space (Fonov et al., 2009) using linear interpolation.

### Fiber Orientation Estimation and Tractography

Multi-tissue fiber response functions were loaded from precomputed files — group-average medians computed from a balanced sample of 1,426 subjects. Fiber orientation distributions (FODs) were estimated via constrained spherical deconvolution (CSD; Tournier et al., 2004, 2008) using the multi-shell multi-tissue variant (MSMT-CSD; Jeurissen et al., 2014) as implemented in MRTrix3 (Tournier et al., 2019). Maximum spherical harmonic order was set to 8 for all three tissue compartments (white matter, grey matter, and cerebrospinal fluid). FODs were intensity-normalized using mtnormalize (Raffelt et al., 2017).

Five-tissue-type (5TT) segmentation images were generated from FreeSurfer outputs using the hybrid surface–volume segmentation (HSVS) algorithm (Smith et al., 2020). Two whole-brain tractography approaches were employed with anatomical constraints (ACT; Smith et al., 2012). Probabilistic tractography was performed using the iFOD2 algorithm (Tournier et al., 2010), generating 1,000,000 streamlines with lengths between 30 and 250 mm, with backtracking enabled and cropping at the grey matter–white matter interface. Deterministic tractography was performed using the SD_Stream algorithm with the same streamline count and length constraints. Both tractograms were filtered using SIFT2 (Smith et al., 2015) to improve the biological plausibility of streamline weights.

### Structural Connectivity

Structural connectivity matrices were constructed from both probabilistic and deterministic tractograms. The following atlases were used: the Schaefer Supplemented with Subcortical Structures (4S) atlas at 156-parcel resolution (Schaefer et al., 2018; Pauli et al., 2018; King et al., 2019; Najdenovska et al., 2018; Glasser et al., 2013), and the Schaefer 100-parcel 7-network parcellation combined with the Tian subcortical atlas at scale S1 (Schaefer et al., 2018; Tian et al., 2020). Cortical parcellations were mapped from template space to diffusion space using the T1w-based spatial normalization. For each atlas and tractography method, four connectivity measures were computed: (1) SIFT2-weighted streamline count with inverse-node-volume scaling, (2) mean streamline length, (3) raw streamline count, and (4) SIFT2-weighted streamline count. All matrices were symmetrized, used a search radius of 2 mm for streamline–parcel assignment, and retained self-connections (non-zero diagonal).

### White Matter Bundle Tractometry

Along-tract profiling was performed on the probabilistic (iFOD2) tractogram using pyAFQ (Kruper et al., 2021; Yeatman et al., 2012). The AFQ segmentation algorithm was applied to identify approximately 24 major white matter bundles. Tissue-property profiles were sampled at 100 equidistant nodes along each bundle using Gaussian weighting. Fractional anisotropy (FA) and mean diffusivity (MD) from diffusion tensor imaging were extracted as scalar measures. Key segmentation parameters included a distance threshold of 3 mm, distance-to-atlas threshold of 4 mm, minimum of 20 streamlines per bundle, and streamline length range of 50–250 mm.

## References

Abraham, A., Pedregosa, F., Eickenberg, M., Gervais, P., Mueller, A., Kossaifi, J., Gramfort, A., Thirion, B., & Varoquaux, G. (2014). Machine learning for neuroimaging with scikit-learn. *Frontiers in Neuroinformatics*, 8, 14.

Avants, B. B., Epstein, C. L., Grossman, M., & Gee, J. C. (2008). Symmetric diffeomorphic image registration with cross-correlation: Evaluating automated labeling of elderly and neurodegenerative brain. *Medical Image Analysis*, 12(1), 26–41.

Cieslak, M., Cook, P. A., He, X., Yeh, F.-C., Dhollander, T., Adebimpe, A., ... & Satterthwaite, T. D. (2021). QSIPrep: An integrative platform for preprocessing and reconstructing diffusion MRI data. *Nature Methods*, 18(7), 775–778.

Daducci, A., Canales-Rodríguez, E. J., Zhang, H., Dyrby, T. B., Alexander, D. C., & Thiran, J.-P. (2015). Accelerated Microstructure Imaging via Convex Optimization (AMICO) from diffusion MRI data. *NeuroImage*, 105, 32–44.

Fischl, B. (2012). FreeSurfer. *NeuroImage*, 62(2), 774–781.

Fonov, V. S., Evans, A. C., McKinstry, R. C., Almli, C. R., & Collins, D. L. (2009). Unbiased nonlinear average age-appropriate brain templates from birth to adulthood. *NeuroImage*, 47(Supplement 1), S102.

Garyfallidis, E., Brett, M., Amirbekian, B., Rokem, A., van der Walt, S., Descoteaux, M., & Nimmo-Smith, I. (2014). Dipy, a library for the analysis of diffusion MRI data. *Frontiers in Neuroinformatics*, 8, 8.

Glasser, M. F., Sotiropoulos, S. N., Wilson, J. A., Coalson, T. S., Fischl, B., Andersson, J. L., ... & Jenkinson, M. (2013). The minimal preprocessing pipelines for the Human Connectome Project. *NeuroImage*, 80, 105–124.

Gorgolewski, K. J., Burns, C. D., Madison, C., Clark, D., Halchenko, Y. O., Waskom, M. L., & Ghosh, S. (2011). Nipype: A flexible, lightweight and extensible neuroimaging data processing framework in Python. *Frontiers in Neuroinformatics*, 5, 13.

Gorgolewski, K. J., Esteban, O., Markiewicz, C. J., Ziegler, E., Ellis, D. G., Notter, M. P., ... & Ghosh, S. (2018). Nipype. *Software*. Zenodo. https://doi.org/10.5281/zenodo.596855

Gorgolewski, K. J., Auer, T., Calhoun, V. D., Craddock, R. C., Das, S., Duff, E. P., ... & Poldrack, R. A. (2016). The brain imaging data structure, a format for organizing and describing outputs of neuroimaging experiments. *Scientific Data*, 3, 160044.

Halchenko, Y. O., Goncalves, M., Ghosh, S., Velasco, P., Visconti di Oleggio Castello, M., Salo, T., ... & Hanke, M. (2024). HeuDiConv — flexible DICOM conversion into structured directory layouts. *Journal of Open Source Software*, 9(99), 5839.

Jenkinson, M., Beckmann, C. F., Behrens, T. E. J., Woolrich, M. W., & Smith, S. M. (2012). FSL. *NeuroImage*, 62(2), 782–790.

Jensen, J. H., Helpern, J. A., Ramani, A., Lu, H., & Kaczynski, K. (2005). Diffusional kurtosis imaging: The quantification of non-Gaussian water diffusion by means of magnetic resonance imaging. *Magnetic Resonance in Medicine*, 53(6), 1432–1440.

Jeurissen, B., Tournier, J.-D., Dhollander, T., Connelly, A., & Sijbers, J. (2014). Multi-tissue constrained spherical deconvolution for improved analysis of multi-shell diffusion MRI data. *NeuroImage*, 103, 411–426.

King, M., Hernandez-Castillo, C. R., Poldrack, R. A., Ivry, R. B., & Diedrichsen, J. (2019). Functional boundaries in the human cerebellum revealed by a multi-domain task battery. *Nature Neuroscience*, 22(8), 1371–1378.

Kruper, J., Yeatman, J. D., Richie-Halford, A., Bloom, D., Grotheer, M., Caffarra, S., ... & Rokem, A. (2021). Evaluating the reliability of human brain white matter tractometry. *Aperture Neuro*, 1(1).

Li, X., Morgan, P. S., Ashburner, J., Smith, J., & Rorden, C. (2016). The first step for neuroimaging data analysis: DICOM to NIfTI conversion. *Journal of Neuroscience Methods*, 264, 47–56.

Najdenovska, E., Alemán-Gómez, Y., Battistella, G., Descoteaux, M., Hagmann, P., Jacquemont, S., ... & Bach Cuadra, M. (2018). In-vivo probabilistic atlas of human thalamic nuclei based on diffusion-weighted magnetic resonance imaging. *Scientific Data*, 5, 180270.

Ozarslan, E., Koay, C. G., Shepherd, T. M., Komlosh, M. E., İrfanoğlu, M. O., Pierpaoli, C., & Basser, P. J. (2013). Mean apparent propagator (MAP) MRI: A novel diffusion imaging method for mapping tissue microstructure. *NeuroImage*, 78, 16–32.

Parker, C. S., Veale, T., Bocchetta, M., Slattery, C. F., Malone, I. B., Thomas, D. L., Schott, J. M., Cash, D. M., & Zhang, H. (2021). Not all voxels are created equal: Reducing estimation bias in regional NODDI metrics using tissue-weighted means. *NeuroImage*, 245, 118749.

Pauli, W. M., Nili, A. N., & Tyszka, J. M. (2018). A high-resolution probabilistic in vivo atlas of human subcortical brain nuclei. *Scientific Data*, 5, 180063.

Raffelt, D., Dhollander, T., Tournier, J.-D., Tabbara, R., Smith, R. E., Pierre, E., & Connelly, A. (2017). Bias field correction and intensity normalisation for quantitative analysis of apparent fibre density. In *Proceedings of the International Society for Magnetic Resonance in Medicine* (Vol. 25, p. 3541).

Reuter, M., Schmansky, N. J., Rosas, H. D., & Fischl, B. (2012). Within-subject template estimation for unbiased longitudinal image analysis. *NeuroImage*, 61(4), 1402–1418.

Schaefer, A., Kong, R., Gordon, E. M., Laumann, T. O., Zuo, X.-N., Holmes, A. J., ... & Yeo, B. T. T. (2018). Local-global parcellation of the human cerebral cortex from intrinsic functional connectivity MRI. *Cerebral Cortex*, 28(9), 3095–3114.

Smith, R. E., Tournier, J.-D., Calamante, F., & Connelly, A. (2012). Anatomically-constrained tractography: Improved diffusion MRI streamlines tractography through effective use of anatomical information. *NeuroImage*, 62(3), 1924–1938.

Smith, R. E., Tournier, J.-D., Calamante, F., & Connelly, A. (2015). SIFT2: Enabling dense quantitative assessment of brain white matter connectivity using streamlines tractography. *NeuroImage*, 119, 338–351.

Smith, R. E., Connelly, A., & Calamante, F. (2020). Hybrid surface-volume segmentation for improved anatomically-constrained tractography. In *Proceedings of the International Society for Magnetic Resonance in Medicine* (Vol. 28).

Tian, Y., Margulies, D. S., Breakspear, M., & Zalesky, A. (2020). Topographic organization of the human subcortex unveiled with functional connectivity gradients. *Nature Neuroscience*, 23(11), 1421–1432.

Tournier, J.-D., Calamante, F., Gadian, D. G., & Connelly, A. (2004). Direct estimation of the fiber orientation density function from diffusion-weighted MRI data using spherical deconvolution. *NeuroImage*, 23(3), 1176–1185.

Tournier, J.-D., Yeh, C.-H., Calamante, F., Cho, K.-H., Connelly, A., & Lin, C.-P. (2008). Resolving crossing fibres using constrained spherical deconvolution: Validation using diffusion-weighted imaging phantom data. *NeuroImage*, 42(2), 617–625.

Tournier, J.-D., Calamante, F., & Connelly, A. (2010). Improved probabilistic streamlines tractography by 2nd order integration over fibre orientation distributions. In *Proceedings of the International Society for Magnetic Resonance in Medicine* (Vol. 18, p. 1670).

Tournier, J.-D., Smith, R., Raffelt, D., Tabbara, R., Dhollander, T., Pietsch, M., ... & Connelly, A. (2019). MRtrix3: A fast, flexible and open software framework for medical image processing and visualisation. *NeuroImage*, 202, 116137.

Yeatman, J. D., Dougherty, R. F., Myall, N. J., Wandell, B. A., & Feldman, H. M. (2012). Tract profiles of white matter properties: Automating fiber-tract quantification. *PLoS ONE*, 7(11), e49790.

Yeh, F.-C., Wedeen, V. J., & Tseng, W.-Y. I. (2010). Generalized q-sampling imaging. *IEEE Transactions on Medical Imaging*, 29(9), 1626–1635.

Zhang, H., Schneider, T., Wheeler-Kingshott, C. A., & Alexander, D. C. (2012). NODDI: Practical in vivo neurite orientation dispersion and density imaging of the human brain. *NeuroImage*, 61(4), 1000–1016.
